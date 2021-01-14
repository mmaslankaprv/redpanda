// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/follower_stats.h"

#include "raft/configuration.h"

namespace raft {
void follower_stats::update_with_configuration(const group_configuration& cfg) {
    cfg.for_each_broker_id([this](const vnode& rni) {
        if (rni == _self || _followers.contains(rni)) {
            return;
        }
        _followers.emplace(rni, follower_index_metadata(rni));
    });

    for (auto it = _followers.cbegin(); it != _followers.cend();) {
        if (!cfg.contains(it->first)) {
            _followers.erase(it++);
        } else {
            ++it;
        }
    }
}
std::ostream& operator<<(std::ostream& o, const follower_stats& s) {
    o << "{followers:" << s._followers.size() << ", [";
    for (auto& f : s) {
        o << f.second;
    }
    return o << "]}";
}

} // namespace raft
