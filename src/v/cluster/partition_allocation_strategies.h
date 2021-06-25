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

#include "cluster/partition_allocator_new.h"
#include "cluster/types.h"
#include "model/metadata.h"

namespace cluster {

class round_robin_allocation_strategy : public allocation_strategy::impl {
public:
    result<allocation_units> allocate_partitions(
      const allocation_configuration&, allocation_state&) final;

private:
    int _next_idx{0};
};

class simple_decommissioned_deallocation_strategy
  : public reallocation_strategy::impl {
public:
    result<allocation_units> reallocate_partitions(
      const std::vector<model::node_id>&,
      const partition_assignment&,
      allocation_state&) final;
};

} // namespace cluster
