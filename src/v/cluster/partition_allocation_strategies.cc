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

#include "cluster/partition_allocation_strategies.h"

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_set.h>
#include <fmt/ostream.h>

#include <vector>

namespace cluster {

result<allocation_units> round_robin_allocation_strategy::allocate_partitions(
  const allocation_configuration& cfg, allocation_state& state) {
    std::vector<partition_assignment> assignments;
    absl::node_hash_set<model::partition_id> custom_assigned;
    // handle custom assignment first
    for (auto& cal : cfg.custom_allocations) {
        std::vector<model::broker_shard> replicas;
        replicas.reserve(cal.nodes.size());

        for (auto n : cal.nodes) {
            auto shard = state.allocate(n);
            if (!shard) {
                // rollback what we currently have and return an error
                state.rollback(assignments);
                state.rollback(replicas);
                return shard.error();
            }
            replicas.push_back(
              model::broker_shard{.node_id = n, .shard = shard.value()});
        }
        partition_assignment assignment{
          .group = state.next_group_id(),
          .id = cal.id,
          .replicas = std::move(replicas)};

        assignments.push_back(std::move(assignment));
        custom_assigned.emplace(cal.id);
    }

    // handle automatic assignment
    for (model::partition_id pid{0}; pid < cfg.partition_count; ++pid) {
        // skip if partition is already assigned
        if (custom_assigned.contains(pid)) {
            continue;
        }
        std::vector<model::broker_shard> partition_replicas;
        partition_replicas.reserve(cfg.replication_factor);

        auto it = state.allocation_nodes().begin();

        // adjust iterator so we will continue where we left off
        if (_next_idx < state.allocation_nodes().size()) {
            it = std::next(it, _next_idx);
        } else {
            _next_idx = 0;
        }
        int attempts = 0;
        while (partition_replicas.size() < (size_t)cfg.replication_factor) {
            // we went over all allocation nodes, terminate allocation with an
            // error

            if (attempts >= state.allocation_nodes().size()) {
                state.rollback(assignments);
                state.rollback(partition_replicas);
                return errc::topic_invalid_partitions;
            }
            fmt::print(
              std::cout,
              "allocating {}/{} at {} [{}]\n",
              pid(),
              cfg.partition_count,
              it->first,
              it->second->partition_capacity());
            if (!it->second->is_decommissioned() && !it->second->is_full()) {
                auto shard = state.allocate(it->first);

                if (shard) {
                    partition_replicas.push_back(model::broker_shard{
                      .node_id = it->first, .shard = shard.value()});
                }
            }

            ++_next_idx;
            ++it;
            ++attempts;
            if (it == state.allocation_nodes().end()) {
                it = state.allocation_nodes().begin();
                _next_idx = 0;
            }
        }
        fmt::print(
          std::cout,
          "pid {}/{} replicas: {}\n",
          pid(),
          cfg.partition_count,
          partition_replicas);
        assignments.push_back(partition_assignment{
          .group = state.next_group_id(),
          .id = pid,
          .replicas = std::move(partition_replicas)});
    }
    return allocation_units(std::move(assignments), &state);
}

result<allocation_units>
simple_decommissioned_deallocation_strategy::reallocate_partitions(
  const std::vector<model::node_id>&,
  const partition_assignment&,
  allocation_state&) {
    return errc::partition_already_exists;
}

} // namespace cluster
