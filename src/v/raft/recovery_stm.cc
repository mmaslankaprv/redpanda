// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/recovery_stm.h"

#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "outcome_future_utils.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <chrono>
#include <optional>

namespace raft {
using namespace std::chrono_literals;

recovery_stm::recovery_stm(
  consensus* p, vnode node_id, scheduling_config scheduling)
  : _ptr(p)
  , _node_id(node_id)
  , _term(_ptr->term())
  , _scheduling(scheduling)
  , _ctxlog(
      raftlog,
      ssx::sformat(
        "[follower: {}] [group_id:{}, {}]",
        _node_id,
        _ptr->group(),
        _ptr->ntp()))
  , _max_inflight_requests(
      config::shard_local_cfg()
        .raft_max_concurrent_append_requests_per_follower) {}

ss::future<> recovery_stm::recover() {
    if (_ptr->_as.abort_requested()) {
        return ss::now();
    }
    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        return ss::now();
    }
    auto sg = meta.value()->is_learner ? _scheduling.learner_recovery_sg
                                       : _scheduling.default_sg;
    auto iopc = meta.value()->is_learner ? _scheduling.learner_recovery_iopc
                                         : _scheduling.default_iopc;

    return ss::with_scheduling_group(
      sg, [this, iopc] { return do_recover(iopc); });
}

ss::future<> recovery_stm::do_recover(ss::io_priority_class iopc) {
    // We have to send all the records that leader have, event those that are
    // beyond commit index, thanks to that after majority have recovered
    // leader can update its commit index
    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        co_return;
    }
    if (_next_follower_offset < model::offset(0)) {
        vlog(
          _ctxlog.trace,
          "initializing follower next offset to {}",
          meta.value()->next_index);
        _next_follower_offset = meta.value()->next_index;
    }

    auto lstats = _ptr->_log.offsets();
    // follower last index was already evicted at the leader, use snapshot
    if (_next_follower_offset < _ptr->_last_snapshot_index) {
        co_return co_await install_snapshot();
    }

    /**
     * We have to store committed_index before doing read as we perform
     * recovery without holding consensus op_lock. Storing committed index
     * to use in append entries request sent during recovery will guarantee
     * correctness. If we would read data before storing committed index we
     * could incorrectly advance follower commit index with data that
     * were actually incorrect.
     *
     * Consider following scenario:
     *
     * Fiber 1 (f1: recovery) - read some data for follower recovery
     * Fiber 2 (f2: request handling) - received append entries request from
     * new
     *                                  leader.
     * 1. f1: start recovery
     * 2. f1: read batches for recovery
     * 3. f2: received append entries
     * 4. f2: truncated log
     * 5. f2: appended to log
     * 6. f2: updated committed_index
     * 7. f1: read committed offset
     * 8. f1: create append entries request
     * 9. f1: send batches to follower
     * 10. f1: stop recovery - not longer a leader
     *
     * In step 9. follower will receive request that will cause the committed
     * offset to be update event though the batches were truncated on the node
     * which sent the request
     *
     */
    _committed_offset = _ptr->committed_offset();

    auto follower_committed_match_index = meta.value()->match_committed_index();
    auto is_learner = meta.value()->is_learner;

    // we do not have next entry for the follower yet, wait for next disk append
    // of follower state change
    if (lstats.dirty_offset < _next_follower_offset) {
        vlog(
          _ctxlog.trace,
          "Recovery status: waiting for node {} state change",
          _node_id);
        co_await meta.value()
          ->follower_state_change
          .wait([this] {
              return state_changed() || _ptr->_transferring_leadership;
          })
          .handle_exception_type([this](const ss::broken_condition_variable&) {
              _stop_requested = true;
          })
          .then([this] {
              vlog(
                _ctxlog.trace,
                "Recovery status: finished waiting for node {} state "
                "change",
                _node_id);
          });
    }

    // recovery may already be finished
    if (_last_success && is_recovery_finished()) {
        co_return;
    }

    meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        co_return;
    }

    auto reader = co_await read_range_for_recovery(
      _next_follower_offset, iopc, is_learner);
    // no batches for recovery, do nothing
    if (!reader) {
        co_return;
    }
    // update next follower offset
    _next_follower_offset = details::next_offset(_last_batch_offset);
    auto inflight_units = co_await ss::get_units(_max_inflight_requests, 1);
    auto distpatch_units = co_await _dispatch_lock.get_units();
    // if previous recovery end up with success allow multiple requests to
    // be send to the follower
    auto f = replicate(
      std::move(*reader),
      should_flush(follower_committed_match_index),
      std::move(distpatch_units));
    if (_last_success) {
        auto max_inflight
          = config::shard_local_cfg()
              .raft_max_concurrent_append_requests_per_follower();
        vlog(
          _ctxlog.trace,
          "waiting for the response in background - inflight requests "
          "{}/{}",
          (max_inflight - _max_inflight_requests.available_units()),
          max_inflight);
        (void)f;
        co_return;
    }
    // by default wait for recovery in foreground
    co_await std::move(f);
}

bool recovery_stm::state_changed() {
    auto meta = get_follower_meta();
    if (!meta) {
        return true;
    }
    return _ptr->_log.offsets().dirty_offset >= _next_follower_offset
           || is_recovery_finished();
}

append_entries_request::flush_after_append
recovery_stm::should_flush(model::offset follower_committed_match_index) const {
    auto lstats = _ptr->_log.offsets();

    /**
     * We request follower to flush only when follower is fully caught
     * up and its match committed offset is smaller than last replicated
     * offset with quorum level. This way when follower flush, leader
     * will be able to update committed_index up to the last offset
     * of last batch replicated with quorum_acks consistency level. Recovery STM
     * works outside of the Raft mutex. It is possible that it read batches that
     * were appendend with quorum consistency level but the
     * _last_quorum_replicated_index wasn't yet updated, hence we have to check
     * if last log append was executed with quorum write and if this is true
     * force the flush on follower.
     */

    const bool is_last_batch = _last_batch_offset == lstats.dirty_offset;
    const bool follower_has_batches_to_commit
      = follower_committed_match_index < _ptr->_last_quorum_replicated_index;
    const bool last_replicate_with_quorum = _ptr->_last_write_consistency_level
                                            == consistency_level::quorum_ack;

    return append_entries_request::flush_after_append(
      is_last_batch
      && (follower_has_batches_to_commit || last_replicate_with_quorum));
}

ss::future<std::optional<model::record_batch_reader>>
recovery_stm::read_range_for_recovery(
  model::offset start_offset, ss::io_priority_class iopc, bool is_learner) {
    storage::log_reader_config cfg(
      start_offset,
      model::offset::max(),
      1,
      // 32KB is a modest estimate. It has good batching and it also prevents an
      // OOM situation where we have a lot of raft groups recovering at the same
      // time and all drawing from memory. If this setting proves difficult,
      // we'll need to throttle with a core-local semaphore
      32 * 1024,
      iopc,
      std::nullopt,
      std::nullopt,
      _ptr->_as);

    if (is_learner) {
        // skip cache insertion on miss for learners which are throttled and
        // often catching up from the beginning of the log (e.g. new nodes)
        cfg.skip_batch_cache = true;
    }

    vlog(_ctxlog.trace, "Reading batches, starting from: {}", start_offset);

    // TODO: add timeout of maybe 1minute?
    auto reader = co_await _ptr->_log.make_reader(cfg);
    auto batches = co_await model::consume_reader_to_memory(
      std::move(reader), model::no_timeout);

    if (batches.empty()) {
        vlog(_ctxlog.trace, "Read no batches for recovery, stopping");
        _stop_requested = true;
        co_return std::nullopt;
    }
    vlog(
      _ctxlog.trace,
      "Read batches in range [{},{}] for recovery",
      batches.front().base_offset(),
      batches.back().last_offset());

    auto gap_filled_batches = details::make_ghost_batches_in_gaps(
      start_offset, std::move(batches));
    _base_batch_offset = gap_filled_batches.begin()->base_offset();
    _last_batch_offset = gap_filled_batches.back().last_offset();

    if (is_learner && _ptr->_recovery_throttle) {
        const auto size = std::accumulate(
          gap_filled_batches.cbegin(),
          gap_filled_batches.cend(),
          size_t{0},
          [](size_t acc, const auto& batch) {
              return acc + batch.size_bytes();
          });
        co_await _ptr->_recovery_throttle->get()
          .throttle(size)
          .handle_exception_type([this](const ss::broken_semaphore&) {
              vlog(_ctxlog.info, "Recovery throttling has stopped");
          });
    }

    co_return model::make_foreign_memory_record_batch_reader(
      std::move(gap_filled_batches));
}

ss::future<> recovery_stm::open_snapshot_reader() {
    return _ptr->_snapshot_mgr.open_snapshot().then(
      [this](std::optional<storage::snapshot_reader> rdr) {
          if (rdr) {
              _snapshot_reader = std::make_unique<storage::snapshot_reader>(
                std::move(*rdr));
              return _snapshot_reader->get_snapshot_size().then(
                [this](size_t sz) { _snapshot_size = sz; });
          }
          return ss::now();
      });
}

ss::future<> recovery_stm::send_install_snapshot_request() {
    // send 32KB at a time
    return read_iobuf_exactly(_snapshot_reader->input(), 32_KiB)
      .then([this](iobuf chunk) mutable {
          auto chunk_size = chunk.size_bytes();
          install_snapshot_request req{
            .target_node_id = _node_id,
            .term = _ptr->term(),
            .group = _ptr->group(),
            .node_id = _ptr->_self,
            .last_included_index = _ptr->_last_snapshot_index,
            .file_offset = _sent_snapshot_bytes,
            .chunk = std::move(chunk),
            .done = (_sent_snapshot_bytes + chunk_size) == _snapshot_size};

          vlog(
            _ctxlog.trace,
            "Sending install snapshot request, last included index: {}",
            req.last_included_index);
          auto seq = _ptr->next_follower_sequence(_node_id);
          _ptr->update_suppress_heartbeats(
            _node_id, seq, heartbeats_suppressed::yes);
          return _ptr->_client_protocol
            .install_snapshot(
              _node_id.id(),
              std::move(req),
              rpc::client_opts(append_entries_timeout()))
            .then([this](result<install_snapshot_reply> reply) {
                return handle_install_snapshot_reply(
                  _ptr->validate_reply_target_node(
                    "install_snapshot", std::move(reply)));
            })
            .finally([this, seq] {
                _ptr->update_suppress_heartbeats(
                  _node_id, seq, heartbeats_suppressed::no);
            });
      });
}

ss::future<> recovery_stm::close_snapshot_reader() {
    return _snapshot_reader->close().then([this] {
        _snapshot_reader.reset();
        _snapshot_size = 0;
        _sent_snapshot_bytes = 0;
    });
}

ss::future<> recovery_stm::handle_install_snapshot_reply(
  result<install_snapshot_reply> reply) {
    // snapshot delivery failed
    if (reply.has_error() || !reply.value().success) {
        // if snapshot delivery failed, stop recovery to update follower state
        // and retry
        _stop_requested = true;
        return close_snapshot_reader();
    }
    if (reply.value().term > _ptr->_term) {
        return close_snapshot_reader().then(
          [this, term = reply.value().term] { return _ptr->step_down(term); });
    }
    _sent_snapshot_bytes = reply.value().bytes_stored;

    // we will send next chunk as a part of recovery loop
    if (_sent_snapshot_bytes != _snapshot_size) {
        return ss::now();
    }

    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        return ss::make_ready_future<>();
    }

    // snapshot received by the follower, continue with recovery
    (*meta)->match_index = _ptr->_last_snapshot_index;
    (*meta)->next_index = details::next_offset(_ptr->_last_snapshot_index);
    _next_follower_offset = (*meta)->next_index;
    return close_snapshot_reader();
}

ss::future<> recovery_stm::install_snapshot() {
    // open reader if not yet available
    auto f = _snapshot_reader != nullptr ? ss::now() : open_snapshot_reader();
    _last_success = false;
    return f.then([this]() mutable {
        // we are outside of raft operation lock if snapshot isn't yet ready we
        // have to wait for it till next recovery loop
        if (!_snapshot_reader) {
            _stop_requested = true;
            return ss::now();
        }

        return send_install_snapshot_request();
    });
}

ss::future<> recovery_stm::replicate(
  model::record_batch_reader&& reader,
  append_entries_request::flush_after_append flush,
  ss::semaphore_units<> u) {
    return ss::try_with_gate(
      _bg,
      [this, u = std::move(u), reader = std::move(reader), flush]() mutable {
          return do_replicate(std::move(reader), flush, std::move(u))
            .handle_exception([this](const std::exception_ptr& e) {
                vlog(_ctxlog.info, "error during follower recovery - {}", e);
            });
      });
}

ss::future<> recovery_stm::do_replicate(
  model::record_batch_reader&& reader,
  append_entries_request::flush_after_append flush,
  ss::semaphore_units<> u) {
    // collect metadata for append entries request
    // last persisted offset is last_offset of batch before the first one in the
    // reader
    auto prev_log_idx = details::prev_offset(_base_batch_offset);
    model::term_id prev_log_term;

    // get term for prev_log_idx batch
    if (prev_log_idx > _ptr->_last_snapshot_index) {
        prev_log_term = *_ptr->_log.get_term(prev_log_idx);
    } else if (prev_log_idx < model::offset(0)) {
        prev_log_term = model::term_id{};
    } else if (prev_log_idx == _ptr->_last_snapshot_index) {
        prev_log_term = _ptr->_last_snapshot_term;
    } else {
        // no entry for prev_log_idx, fallback to install snapshot
        return install_snapshot();
    }

    // calculate commit index for follower to update immediately
    auto commit_idx = std::min(_last_batch_offset, _committed_offset);
    auto last_visible_idx = std::min(
      _last_batch_offset, _ptr->last_visible_index());
    // build request
    append_entries_request r(
      _ptr->self(),
      _node_id,
      protocol_metadata{
        .group = _ptr->group(),
        .commit_index = commit_idx,
        .term = _term,
        .prev_log_index = prev_log_idx,
        .prev_log_term = prev_log_term,
        .last_visible_index = last_visible_idx},
      std::move(reader),
      flush);

    _ptr->update_node_append_timestamp(_node_id);

    auto seq = _ptr->next_follower_sequence(_node_id);
    _ptr->update_suppress_heartbeats(_node_id, seq, heartbeats_suppressed::yes);
    auto lstats = _ptr->_log.offsets();
    return dispatch_append_entries(std::move(r), std::move(u))
      .finally([this, seq] {
          _ptr->update_suppress_heartbeats(
            _node_id, seq, heartbeats_suppressed::no);
      })
      .then([this, seq, dirty_offset = lstats.dirty_offset](auto r) {
          if (!r) {
              vlog(
                _ctxlog.error,
                "recovery append entries error: {}",
                r.error().message());
              _stop_requested = true;
              _ptr->get_probe().recovery_request_error();
          }
          _ptr->process_append_entries_reply(
            _node_id.id(), r.value(), seq, dirty_offset);
          // If follower stats aren't present we have to stop recovery as
          // follower was removed from configuration
          auto meta = get_follower_meta();
          if (!meta) {
              _stop_requested = true;
              return;
          }
          // If request was reordered we have to stop recovery as follower state
          // is not known
          if (seq < meta.value()->last_received_seq) {
              _stop_requested = true;
              return;
          }
          // move the follower next index backward if recovery were not
          // successfull
          //
          // Raft paper:
          // If AppendEntries fails because of log inconsistency: decrement
          // nextIndex and retry(§5.3)
          append_entries_reply reply = r.value();
          _last_follower_dirty_offset = reply.last_dirty_log_index;
          if (reply.result == append_entries_reply::status::failure) {
              if (_last_follower_dirty_offset > reply.last_dirty_log_index) {
                  _next_follower_offset = meta.value()->next_index;
                  vlog(
                    _ctxlog.trace,
                    "Follower dirty offset moved back, reinitializing next "
                    "offset to: {}",
                    _next_follower_offset);
              }
              meta.value()->next_index = std::max(
                model::offset(0), details::prev_offset(_base_batch_offset));
              _last_success = false;
              _next_follower_offset = meta.value()->next_index;
              vlog(
                _ctxlog.trace,
                "Move next index {} backward",
                meta.value()->next_index);
              return;
          }
          _last_success = true;
      })
      .finally([this] {
          auto meta = get_follower_meta();

          if (!meta) {
              _stop_requested = true;
              return;
          }
          /**
           * since we do not stop recovery for relaxed consistency writes we
           * have to notify recovery_finished condition variable when follower
           * is up to date, but before finishing recovery
           */
          auto max_offset = _ptr->_log.offsets().dirty_offset();
          if (meta.value()->match_index == max_offset) {
              meta.value()->recovery_finished.broadcast();
          }
          meta.value()->follower_state_change.broadcast();
      });
}

clock_type::time_point recovery_stm::append_entries_timeout() {
    return raft::clock_type::now() + _ptr->_recovery_append_timeout;
}

ss::future<result<append_entries_reply>> recovery_stm::dispatch_append_entries(
  append_entries_request&& r, [[maybe_unused]] ss::semaphore_units<> u) {
    _ptr->_probe.recovery_append_request();
    rpc::client_opts opts(append_entries_timeout());
    std::vector<ss::semaphore_units<>> units_v;
    units_v.push_back(std::move(u));
    auto units
      = ss::make_foreign<ss::lw_shared_ptr<std::vector<ss::semaphore_units<>>>>(
        ss::make_lw_shared<std::vector<ss::semaphore_units<>>>(
          std::move(units_v)));
    opts.resource_units = std::move(units);
    return _ptr->_client_protocol
      .append_entries(_node_id.id(), std::move(r), std::move(opts))
      .then([this](result<append_entries_reply> reply) {
          return _ptr->validate_reply_target_node(
            "append_entries_recovery", std::move(reply));
      });
}

bool recovery_stm::is_recovery_finished() {
    /**
     * finish recovery loop when:
     *
     * preliminary checks:
     *  1) consensus is shutting down
     *  2) stop was requested
     *  3) term changed
     *  4) node is not longer a leader
     *  5) follower was deleted
     *
     */
    if (
      _ptr->_as.abort_requested() || _ptr->_bg.is_closed() || _stop_requested
      || _term != _ptr->term() || !_ptr->is_leader()) {
        return true;
    }

    auto meta = get_follower_meta();
    if (!meta) {
        return true;
    }

    auto lstats = _ptr->_log.offsets();
    auto log_end_offset = lstats.dirty_offset();

    bool is_up_to_date = meta.value()->match_index == log_end_offset;
    bool quorum_writes = _ptr->_visibility_upper_bound_index
                         <= _ptr->_commit_index;
    /**
     * We do not stop recovery for relaxed consistency recoveries as we want
     * recoveries to be send immediately after leader disk append. For low
     * volume producers we might have to wait for the next heartbeat to send
     * recovery request to follower which would lead to increased E2E latency
     *
     * The exception to this is leadership transfer: if our offset is up to
     * date and leadership transfer is up to date, the transfer is probably
     * waiting for us to complete, so we drop out.
     */
    return (is_up_to_date && (quorum_writes || _ptr->_transferring_leadership));
}

ss::future<> recovery_stm::apply() {
    return recover()
      .then([this] {
          return ss::do_until(
            [this] { return is_recovery_finished(); },
            [this] { return recover(); });
      })
      .finally([this] {
          vlog(_ctxlog.trace, "Finished recovery");
          auto f = _bg.close();
          auto meta = get_follower_meta();
          if (meta) {
              meta.value()->is_recovering = false;
              // set stop requested flag to skip checking if follower is up to
              // date
              _stop_requested = true;
              meta.value()->recovery_finished.broadcast();
          }
          if (_snapshot_reader != nullptr) {
              f = f.then([this] { return close_snapshot_reader(); });
          }
          return f;
      });
}

std::optional<follower_index_metadata*> recovery_stm::get_follower_meta() {
    auto it = _ptr->_fstats.find(_node_id);
    if (it == _ptr->_fstats.end()) {
        vlog(_ctxlog.info, "Node is not longer in followers list");
        return std::nullopt;
    }
    return &it->second;
}

} // namespace raft
