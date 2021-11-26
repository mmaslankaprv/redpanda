/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/metadata.h"
#include "outcome.h"
#include "raft/logger.h"
#include "storage/snapshot.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/gate.hh>

namespace raft {

class recovery_stm {
public:
    recovery_stm(consensus*, vnode, scheduling_config);
    ss::future<> apply();

private:
    ss::future<> recover();
    ss::future<> do_recover(ss::io_priority_class);
    ss::future<std::optional<model::record_batch_reader>>
    read_range_for_recovery(model::offset, ss::io_priority_class, bool);

    ss::future<> replicate(
      model::record_batch_reader&&,
      append_entries_request::flush_after_append,
      ss::semaphore_units<>);
    ss::future<> do_replicate(
      model::record_batch_reader&&,
      append_entries_request::flush_after_append,
      ss::semaphore_units<>);
    ss::future<result<append_entries_reply>>
    dispatch_append_entries(append_entries_request&&, ss::semaphore_units<>);
    std::optional<follower_index_metadata*> get_follower_meta();
    clock_type::time_point append_entries_timeout();

    ss::future<> install_snapshot();
    ss::future<> send_install_snapshot_request();
    ss::future<> handle_install_snapshot_reply(result<install_snapshot_reply>);
    ss::future<> open_snapshot_reader();
    ss::future<> close_snapshot_reader();
    bool state_changed();
    bool is_recovery_finished();
    append_entries_request::flush_after_append
      should_flush(model::offset) const;
    consensus* _ptr;
    vnode _node_id;
    model::offset _base_batch_offset;
    model::offset _last_batch_offset;
    model::offset _committed_offset;
    model::term_id _term;
    scheduling_config _scheduling;
    prefix_logger _ctxlog;
    // tracking follower snapshot delivery
    std::unique_ptr<storage::snapshot_reader> _snapshot_reader;
    size_t _sent_snapshot_bytes = 0;
    size_t _snapshot_size = 0;
    // needed to early exit. (node down)
    bool _stop_requested = false;
    /**
     * we hold dispatch lock to make sure that only one request is processed at
     * a time in recovery_stm, but multiple requests are handed off to rpc
     * layer.
     */
    mutex _dispatch_lock;
    /**
     * internal state to track next follower offset, it is updated based on the
     * range read for recovery. this represent next offset that is expected by
     * follower when there are no failures and raft group is in steady state
     */
    model::offset _next_follower_offset;
    model::offset _last_follower_dirty_offset;
    bool _last_success = false;
    ss::gate _bg;
    /**
     * Simple semaphore limiting number of max inflight follower requests.
     *
     * TODO: consider removing it when we will have proper backpressure
     * propagation in RPC
     */
    ss::semaphore _max_inflight_requests;
};

} // namespace raft
