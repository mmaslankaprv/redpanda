// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/follower_stats.h"

namespace raft {
void follower_stats::update_with_configuration(const group_configuration& cfg) {
    cfg.for_each_broker([this](const model::broker& n) {
        if (n.id() == _self || _followers.contains(n.id())) {
            return;
        }
        _followers.emplace(n.id(), follower_index_metadata(n.id()));
    });

    for (auto it = _followers.cbegin(); it != _followers.cend();) {
        if (!cfg.contains_broker(it->first)) {
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
