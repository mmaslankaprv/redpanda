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

#include "storage/fwd.h"
#include "utils/backlog_controller.h"

#include <seastar/core/sharded.hh>

#include <cstddef>
#include <cstdint>

namespace storage {

struct compaction_backlog_sampler : public backlog_controller::sampler {
    explicit compaction_backlog_sampler(ss::sharded<log_manager>&);

    ss::future<int64_t> sample_backlog() final;

private:
    ss::sharded<log_manager>& _log_manager;
};

} // namespace storage
