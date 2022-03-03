
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
#include "cluster/fwd.h"
#include "cluster/migration.h"
#include "kafka/server/group_router.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <chrono>

#pragma once

namespace kafka {
struct group_metadata_migration : cluster::migration::impl {
public:
    explicit group_metadata_migration(
      cluster::controller&, ss::sharded<group_router>&);
    bool is_required() final;
    ss::future<> apply(cluster::migration::context) final;
    std::string_view name() const final { return "consumer group metadata"; }

private:
    static constexpr auto default_timeout = std::chrono::seconds(5);

    model::timeout_clock::time_point default_deadline() {
        return model::timeout_clock::now() + default_timeout;
    }

    void dispatch_ntp_migration(model::ntp);
    ss::future<> do_dispatch_ntp_migration(model::ntp);
    cluster::controller& _controller;
    ss::sharded<group_router>& _router;
    ss::gate _partitions_gate;
};

cluster::migration
make_group_metadata_migration(cluster::controller&, ss::sharded<group_router>&);
} // namespace kafka
