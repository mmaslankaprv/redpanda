// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_allocator_new.h"

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <absl/container/node_hash_map.h>
#include <fmt/core.h>
#include <fmt/ostream.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <vector>

namespace cluster {
allocation_node::allocation_node(
  model::node_id id,
  uint32_t cpus,
  absl::node_hash_map<ss::sstring, ss::sstring> labels)
  : _id(id)
  , _weights(cpus)
  , _max_capacity((cpus * max_allocations_per_core) - core0_extra_weight)
  , _machine_labels(std::move(labels))
  , _decommissioned(false) {
    // add extra weights to core 0
    _weights[0] = core0_extra_weight;
}

uint32_t allocation_node::allocate() {
    auto it = std::min_element(_weights.begin(), _weights.end());
    (*it)++; // increment the weights
    _allocated_partitions++;
    return std::distance(_weights.begin(), it);
}
void allocation_node::deallocate(uint32_t core) {
    vassert(
      core < _weights.size(),
      "Tried to deallocate a non-existing core:{} - {}",
      core,
      *this);
    vassert(
      _allocated_partitions > 0 && _weights[core] > 0,
      "unable to deallocate partition from core {} at node {}",
      core,
      *this);

    _allocated_partitions--;
    _weights[core]--;
}
void allocation_node::allocate(uint32_t core) {
    vassert(
      core < _weights.size(),
      "Tried to allocate a non-existing core:{} - {}",
      core,
      *this);
    _weights[core]++;
    _allocated_partitions++;
}
const absl::node_hash_map<ss::sstring, ss::sstring>&
allocation_node::machine_labels() const {
    return _machine_labels;
}

void allocation_state::rollback(const std::vector<partition_assignment>& v) {
    for (auto& as : v) {
        rollback(as.replicas);
        // rollback for each assignment as the groups are distinct
        _highest_group = raft::group_id(_highest_group() - 1);
    }
}

void allocation_state::rollback(const std::vector<model::broker_shard>& v) {
    for (auto& bs : v) {
        deallocate(bs);
    }
}
int16_t allocation_state::available_nodes() const {
    return std::count_if(
      _nodes.begin(), _nodes.end(), [](const underlying_t::value_type& n) {
          return !n.second->is_decommissioned();
      });
}

raft::group_id allocation_state::next_group_id() { return ++_highest_group; }

void allocation_state::apply_update(
  std::vector<model::broker_shard> replicas, raft::group_id group_id) {
    if (replicas.empty()) {
        return;
    }
    _highest_group = std::max(_highest_group, group_id);
    // We can use non stable sort algorithm as we do not need to preserver
    // the order of shards
    std::sort(
      replicas.begin(),
      replicas.end(),
      [](const model::broker_shard& l, const model::broker_shard& r) {
          return l.node_id > r.node_id;
      });
    auto node_id = std::cbegin(replicas)->node_id;
    auto it = _nodes.find(node_id);

    for (auto const& bs : replicas) {
        if (it == _nodes.end()) {
            // do nothing, node was deleted
            continue;
        }
        // Thanks to shards being sorted we need to do only
        //  as many lookups as there are brokers
        if (it->first != bs.node_id) {
            it = _nodes.find(bs.node_id);
        }
        if (it != _nodes.end()) {
            it->second->allocate(bs.shard);
        }
    }
}

void allocation_state::register_node(allocation_state::node_ptr n) {
    _nodes.emplace(n->_id, std::move(n));
}

void allocation_state::unregister_node(model::node_id id) { _nodes.erase(id); }

void allocation_state::decommission_node(model::node_id id) {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    // mark as decommissioned
    it->second->decommission();
}

void allocation_state::recommission_node(model::node_id id) {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    // mark as recommissioned
    it->second->recommission();
}

bool allocation_state::is_empty(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    return it->second->empty();
}

void allocation_state::deallocate(const model::broker_shard& replica) {
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->deallocate(replica.shard);
    }
}

result<uint32_t> allocation_state::allocate(model::node_id id) {
    if (auto it = _nodes.find(id); it != _nodes.end()) {
        if (it->second->is_full() || it->second->is_decommissioned()) {
            return errc::invalid_node_opeartion;
        }
        return it->second->allocate();
    }

    return errc::node_does_not_exists;
}

partition_allocator::partition_allocator(
  allocation_strategy allocation_strategy,
  reallocation_strategy reallocation_strategy)
  : _state(std::make_unique<allocation_state>())
  , _allocation_strategy(std::move(allocation_strategy))
  , _reallocation_strategy(std::move(reallocation_strategy)) {}

void partition_allocator::deallocate(
  const std::vector<model::broker_shard>& replicas) {
    for (auto& r : replicas) {
        // find in brokers
        _state->deallocate(r);
    }
}

void partition_allocator::update_allocation_state(
  const std::vector<model::topic_metadata>& metadata, raft::group_id gid) {
    if (metadata.empty()) {
        return;
    }

    std::vector<model::broker_shard> shards;
    for (auto const& t_md : metadata) {
        for (auto& p_md : t_md.partitions) {
            std::move(
              p_md.replicas.begin(),
              p_md.replicas.end(),
              std::back_inserter(shards));
        }
    }
    _state->apply_update(shards, gid);
}
result<allocation_units> partition_allocator::reassign_decommissioned_replicas(
  const partition_assignment& current) {
    std::vector<model::node_id> decommissioned;
    for (auto& n : _state->allocation_nodes()) {
        if (n.second->is_decommissioned()) {
            decommissioned.push_back(n.first);
        }
    }

    return _reallocation_strategy.reallocate_partitions(
      decommissioned, current, *_state);
}

std::ostream& operator<<(std::ostream& o, const allocation_node& n) {
    o << "{ node:" << n._id << ", max_partitions_per_core: "
      << allocation_node::max_allocations_per_core
      << ", partition_capacity:" << n.partition_capacity() << ", weights: [";
    for (auto w : n._weights) {
        o << "(" << w << ")";
    }
    return o << "]}";
}

std::ostream& operator<<(std::ostream& o, const custom_allocation& a) {
    fmt::print(o, "{{partition_id: {}, nodes: {} }}", a.id, a.nodes);
    return o;
}

std::ostream& operator<<(std::ostream& o, const allocation_configuration& a) {
    fmt::print(
      o,
      "{{partition_count: {}, replication_factor: {}, custom_allocations: {} "
      "}}",
      a.partition_count,
      a.replication_factor,
      a.custom_allocations);
    return o;
}

} // namespace cluster
