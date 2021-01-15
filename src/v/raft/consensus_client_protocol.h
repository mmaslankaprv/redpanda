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
#include "model/timeout_clock.h"
#include "outcome.h"
#include "raft/types.h"
#include "rpc/types.h"

#include <seastar/core/shared_ptr.hh>

namespace raft {

/// Virtualized Raft client protocol. The protocol allows to communicate
/// with other cluster members.

class consensus_client_protocol final {
public:
    struct impl {
        virtual ss::future<result<vote_reply>>
        vote(vnode, vote_request&&, rpc::client_opts) = 0;

        virtual ss::future<result<append_entries_reply>>
        append_entries(vnode, append_entries_request&&, rpc::client_opts) = 0;

        virtual ss::future<result<heartbeat_reply>>
        heartbeat(vnode, heartbeat_request&&, rpc::client_opts) = 0;

        virtual ss::future<result<install_snapshot_reply>>
        install_snapshot(vnode, install_snapshot_request&&, rpc::client_opts)
          = 0;

        virtual ss::future<result<timeout_now_reply>>
        timeout_now(vnode, timeout_now_request&&, rpc::client_opts) = 0;

        virtual ~impl() noexcept = default;
    };

public:
    explicit consensus_client_protocol(ss::shared_ptr<impl> i)
      : _impl(std::move(i)) {}
    ss::future<result<vote_reply>>
    vote(vnode targe_node, vote_request&& r, rpc::client_opts opts) {
        return _impl->vote(targe_node, std::move(r), std::move(opts));
    }

    ss::future<result<append_entries_reply>> append_entries(
      vnode targe_node, append_entries_request&& r, rpc::client_opts opts) {
        return _impl->append_entries(targe_node, std::move(r), std::move(opts));
    }

    ss::future<result<heartbeat_reply>>
    heartbeat(vnode targe_node, heartbeat_request&& r, rpc::client_opts opts) {
        return _impl->heartbeat(targe_node, std::move(r), std::move(opts));
    }

    ss::future<result<install_snapshot_reply>> install_snapshot(
      vnode target_node, install_snapshot_request&& r, rpc::client_opts opts) {
        return _impl->install_snapshot(
          target_node, std::move(r), std::move(opts));
    }

    ss::future<result<timeout_now_reply>> timeout_now(
      vnode target_node, timeout_now_request&& r, rpc::client_opts opts) {
        return _impl->timeout_now(target_node, std::move(r), std::move(opts));
    }

private:
    ss::shared_ptr<impl> _impl;
};

template<typename Impl, typename... Args>
static consensus_client_protocol
make_consensus_client_protocol(Args&&... args) {
    return consensus_client_protocol(
      ss::make_shared<Impl>(std::forward<Args>(args)...));
}
} // namespace raft
