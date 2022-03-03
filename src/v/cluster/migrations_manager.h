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

#include "cluster/migration.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include <memory>

namespace cluster {

class migrations_manager {
public:
    ss::future<> execute();
    ss::future<> stop() {
        _as.request_abort();
        return _gate.close();
    }

    void register_migration(migration);

private:
    std::vector<migration> _migrations;
    ss::abort_source _as;
    ss::gate _gate;
};
} // namespace cluster
