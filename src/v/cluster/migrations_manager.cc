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

#include "cluster/migrations_manager.h"

#include "cluster/logger.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
namespace cluster {

ss::future<> migrations_manager::execute() {
    for (auto& m : _migrations) {
        if (_as.abort_requested()) {
            co_return;
        }
        if (m.is_required()) {
            vlog(clusterlog.info, "executing migration {}", m.name());
            co_await m.apply(migration::context{
              .gate = _gate,
              .abort_source = _as,
            });
        }
    }
}

void migrations_manager::register_migration(migration m) {
    _migrations.push_back(std::move(m));
}

} // namespace cluster
