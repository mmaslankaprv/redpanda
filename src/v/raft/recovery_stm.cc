// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/recovery_stm.h"

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "outcome_future_utils.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "ssx/sformat.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <chrono>

namespace raft {
using namespace std::chrono_literals;

recovery_stm::recovery_stm(
  consensus* p, vnode node_id, scheduling_config scheduling)
  : _ptr(p)
  , _node_id(node_id)
  , _scheduling(scheduling)
  , _ctxlog(
      raftlog,
      ssx::sformat(
        "[group_id:{}, {}][f: {}]", _ptr->group(), _ptr->ntp(), node_id)) {
    start();
}

ss::future<> recovery_stm::dispatch() {
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
      sg, [this, iopc] { return do_dispatch(iopc); });
}

void recovery_stm::stop() {
    _stop_requested = true;
    _leadership_changed.broken();
}

ss::future<> recovery_stm::do_dispatch(ss::io_priority_class iopc) {
    try {
        co_await _leadership_changed.wait([this] { return _ptr->is_leader(); });
    } catch (const ss::broken_condition_variable& e) {
        _stop_requested = true;
        co_return;
    }
    // We have to send all the records that leader have, event those that are
    // beyond commit index, thanks to that after majority have recovered
    // leader can update its commit index
    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        co_return;
    }
    vlog(
      _ctxlog.trace,
      "follower state: {}, next offset: {}",
      *meta.value(),
      _next_offset);
    if (meta.value()->last_dirty_log_index < _next_offset) {
        _next_offset = details::next_offset(meta.value()->match_index);
    }

    auto lstats = _ptr->_log.offsets();
    // follower last index was already evicted at the leader, use snapshot
    if (_next_offset < lstats.start_offset) {
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
    const auto is_learner = meta.value()->is_learner;

    // we do not have next entry for the follower yet, wait for next disk append
    // of follower state change
    if (lstats.dirty_offset < _next_offset) {
        co_await meta.value()
          ->follower_state_change.wait([this] { return state_changed(); })
          .handle_exception_type([this](const ss::broken_condition_variable&) {
              _stop_requested = true;
          });
    }

    if (meta.value()->last_dirty_log_index < _next_offset) {
        _next_offset = details::next_offset(meta.value()->match_index);
    }
    // read range for follower recovery
    auto batches = co_await read_range(_next_offset, iopc, is_learner);

    if (batches.empty()) {
        co_return;
    }
    // update recovery state
    _base_batch_offset = batches.begin()->base_offset();
    _last_batch_offset = batches.back().last_offset();

    // throttle learners recovery
    if (is_learner && _ptr->_recovery_throttle) {
        const auto size = std::accumulate(
          batches.cbegin(),
          batches.cend(),
          size_t{0},
          [](size_t acc, const auto& batch) {
              return acc + batch.size_bytes();
          });

        co_await _ptr->_recovery_throttle->get()
          .throttle(size)
          .handle_exception_type([this](const ss::broken_semaphore&) {
              vlog(_ctxlog.info, "dispatcher throttling has stopped");
          });
    }

    auto f_reader = model::make_foreign_memory_record_batch_reader(
      std::move(batches));

    co_await replicate(
      std::move(f_reader), should_flush(follower_committed_match_index));

    meta = get_follower_meta();
    if (!meta) {
        _stop_requested = true;
        co_return;
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
}

bool recovery_stm::state_changed() {
    auto meta = get_follower_meta();
    if (!meta) {
        return true;
    }
    return _ptr->_log.offsets().dirty_offset
           > meta.value()->last_dirty_log_index;
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

ss::future<ss::circular_buffer<model::record_batch>> recovery_stm::read_range(
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

    vlog(_ctxlog.trace, "reading batches, starting from {}", start_offset);

    auto rdr = co_await _ptr->_log.make_reader(cfg);

    auto batches = co_await model::consume_reader_to_memory(
      std::move(rdr), model::no_timeout);

    vlog(_ctxlog.trace, "read {} batches", batches.size());

    co_return details::make_ghost_batches_in_gaps(
      start_offset, std::move(batches));
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
            _ctxlog.debug,
            "sending install snapshot request, last included index: {}",
            req.last_included_index);
          return _ptr->_client_protocol
            .install_snapshot(
              _node_id.id(),
              std::move(req),
              rpc::client_opts(append_entries_timeout()))
            .then([this](result<install_snapshot_reply> reply) {
                return handle_install_snapshot_reply(
                  _ptr->validate_reply_target_node(
                    "install_snapshot", std::move(reply)));
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
    _next_offset = details::next_offset(_ptr->_last_snapshot_index);
    return close_snapshot_reader();
}

ss::future<> recovery_stm::install_snapshot() {
    // open reader if not yet available
    auto f = _snapshot_reader != nullptr ? ss::now() : open_snapshot_reader();

    return f.then([this]() mutable {
        // we are outside of raft operation lock if snapshot isn't yet ready we
        // have to wait for it till next recovery loop
        if (!_snapshot_reader) {
            return ss::now();
        }

        return send_install_snapshot_request();
    });
}

ss::future<> recovery_stm::replicate(
  model::record_batch_reader&& reader,
  append_entries_request::flush_after_append flush) {
    // collect metadata for append entries request
    // last persisted offset is last_offset of batch before the first one in the
    // reader
    auto prev_log_idx = details::prev_offset(_base_batch_offset);
    model::term_id prev_log_term;
    auto lstats = _ptr->_log.offsets();

    // get term for prev_log_idx batch
    if (prev_log_idx >= lstats.start_offset) {
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
        .term = _ptr->term(),
        .prev_log_index = prev_log_idx,
        .prev_log_term = prev_log_term,
        .last_visible_index = last_visible_idx},
      std::move(reader),
      flush);

    _ptr->update_node_append_timestamp(_node_id);

    auto seq = _ptr->next_follower_sequence(_node_id);
    _ptr->update_suppress_heartbeats(_node_id, seq, heartbeats_suppressed::yes);
    vlog(
      _ctxlog.trace,
      "sending append entries request: [meta: {}, flush: {}]",
      r.meta,
      r.flush);
    return dispatch_append_entries(std::move(r))
      .finally([this, seq] {
          _ptr->update_suppress_heartbeats(
            _node_id, seq, heartbeats_suppressed::no);
      })
      .then([this, seq, dirty_offset = lstats.dirty_offset](auto r) {
          if (!r) {
              vlog(
                _ctxlog.error,
                "error replicating entry - {}",
                r.error().message());
              _ptr->get_probe().recovery_request_error();
          }
          auto meta = get_follower_meta();
          if (!meta) {
              _stop_requested = true;
              return;
          }
          _ptr->process_append_entries_reply(
            _node_id.id(),
            append_entries_reply_ctx::dispatcher,
            r.value(),
            seq,
            dirty_offset);

          if (r.value().result == append_entries_reply::status::success) {
              _next_offset = details::next_offset(
                meta.value()->last_dirty_log_index);
          }

          // move the follower next index backward if recovery were not
          // successfull
          //
          // Raft paper:
          // If AppendEntries fails because of log inconsistency: decrement
          // nextIndex and retry(§5.3)

          if (r.value().result == append_entries_reply::status::failure) {
              meta.value()->is_recovering = true;
              const auto prev = _next_offset;
              _next_offset = std::max(
                model::offset(0), details::prev_offset(_base_batch_offset));
              vlog(
                _ctxlog.trace,
                "moving next offset backward {} -> {}",
                prev,
                _next_offset);
          }
      });
}

clock_type::time_point recovery_stm::append_entries_timeout() {
    return raft::clock_type::now() + _ptr->_recovery_append_timeout;
}

ss::future<result<append_entries_reply>>
recovery_stm::dispatch_append_entries(append_entries_request&& r) {
    _ptr->_probe.recovery_append_request();

    return _ptr->_client_protocol
      .append_entries(
        _node_id.id(), std::move(r), rpc::client_opts(append_entries_timeout()))
      .then([this](result<append_entries_reply> reply) {
          return _ptr->validate_reply_target_node(
            "append_entries_recovery", std::move(reply));
      });
}

bool recovery_stm::should_stop() {
    if (_ptr->_bg.is_closed()) {
        return true;
    }

    auto meta = get_follower_meta();
    if (!meta) {
        return true;
    }

    return _stop_requested;
}

void recovery_stm::start() {
    vlog(_ctxlog.info, "starting dispatcher");
    (void)ss::with_gate(_ptr->_bg, [this] {
        auto meta = get_follower_meta();
        if (!meta) {
            // stop recovery when node was removed
            return ss::now();
        }
        _next_offset = details::next_offset(meta.value()->last_dirty_log_index);
        return ss::do_until(
          [this] { return should_stop(); },
          [this] {
              return dispatch().handle_exception(
                [this](const std::exception_ptr& e) {
                    vlog(
                      _ctxlog.debug,
                      "exception while dispatching follower request - {}",
                      e);
                });
          });
    }).finally([this] {
        vlog(_ctxlog.info, "stopped dispatcher");
        if (_snapshot_reader != nullptr) {
            return close_snapshot_reader();
        }
        return ss::now();
    });
}

std::optional<follower_index_metadata*> recovery_stm::get_follower_meta() {
    auto it = _ptr->_fstats.find(_node_id);
    if (it == _ptr->_fstats.end()) {
        vlog(_ctxlog.info, "not longer in follower list");
        return std::nullopt;
    }
    return &it->second;
}

} // namespace raft
