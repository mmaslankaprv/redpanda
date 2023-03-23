/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/types.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

class allocation_state;
/**
 * make_soft_constraint adapts hard constraint to soft one by returning
 * max score for nodes that matches the soft constraint and 0 for
 * the ones that not
 */
soft_constraint make_soft_constraint(hard_constraint);

hard_constraint not_fully_allocated();
hard_constraint is_active();

hard_constraint on_node(model::node_id);

hard_constraint on_nodes(const std::vector<model::node_id>&);

hard_constraint distinct_from(const std::vector<model::broker_shard>&);

hard_constraint distinct_nodes();

/*
 * constraint checks that new partition won't violate max_disk_usage_ratio
 * partition_size is size of partition that is going to be allocated
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
hard_constraint disk_not_overflowed_by_partition(
  const double max_disk_usage_ratio,
  const size_t partition_size,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

/*
 * scores nodes based on free overall allocation capacity left
 * returning `0` for fully allocated nodes and `max_capacity` for empty nodes
 */
soft_constraint least_allocated();

/*
 * scores nodes based on allocation capacity used by priority partitions
 * returning `0` for nodes fully allocated for priority partitions
 * and `max_capacity` for nodes without any priority partitions
 * non-priority partition allocations are ignored
 */
soft_constraint least_allocated_in_domain(partition_allocation_domain);

/*
 * constraint scores nodes on free disk space
 * assigned_reallocation_sizes is sizes of partitions that are going to be
 * allocated on node
 */
soft_constraint least_disk_filled(
  const double max_disk_usage_ratio,
  const absl::flat_hash_map<model::node_id, node_disk_space>&
    node_disk_reports);

template<
  typename Mapper,
  typename LabelType
  = typename std::invoke_result_t<Mapper, model::node_id>::value_type>
concept LabelMapper = requires(Mapper mapper, model::node_id id) {
    { mapper(id) } -> std::convertible_to<std::optional<LabelType>>;
};

template<typename Mapper>
requires LabelMapper<Mapper>
  hard_constraint distinct_label(ss::sstring label_name, Mapper&& mapper) {
    class impl : public hard_constraint::impl {
    public:
        impl(ss::sstring label, Mapper&& mapper)
          : _label_name(std::move(label))
          , _mapper(std::forward<Mapper>(mapper)) {}

        hard_constraint_evaluator
        make_evaluator(const replicas_t& current_replicas) const final {
            return
              [this, &current_replicas](const allocation_node& candidate_node) {
                  for (auto [node_id, shard] : current_replicas) {
                      const auto replica_label = _mapper(node_id);

                      // replica has no label assigned, any node will match
                      if (!replica_label.has_value()) {
                          return true;
                      }
                      // label is already in replica set
                      if (replica_label == _mapper(candidate_node.id())) {
                          return false;
                      }
                  }
                  return true;
              };
        }

        ss::sstring name() const final {
            return fmt::format("distinct {}", _label_name);
        }

        ss::sstring _label_name;
        Mapper _mapper;
    };

    return hard_constraint(std::make_unique<impl>(
      std::move(label_name), std::forward<Mapper>(mapper)));
}

template<
  typename Mapper,
  typename T =
    typename std::invoke_result_t<Mapper, model::node_id>::value_type>
requires LabelMapper<Mapper, T> soft_constraint
distinct_labels_preferred(ss::sstring label_name, Mapper&& mapper) {
    class impl : public soft_constraint::impl {
    public:
        impl(ss::sstring label, Mapper&& mapper)
          : _label_name(std::move(label))
          , _mapper(std::forward<Mapper>(mapper)) {}

        soft_constraint_evaluator
        make_evaluator(const replicas_t& current_replicas) const final {
            absl::flat_hash_map<T, size_t> frequency_map;

            for (auto& r : current_replicas) {
                auto const l = _mapper(r.node_id);
                if (!l) {
                    continue;
                }
                auto [it, _] = frequency_map.try_emplace(*l, 0);
                it->second += 1;
            }

            return [this, frequency_map = std::move(frequency_map)](
                     const allocation_node& candidate_node) -> uint64_t {
                auto node_label = _mapper(candidate_node.id());
                if (!node_label) {
                    return (uint64_t)0;
                }
                auto it = frequency_map.find(*node_label);

                if (it == frequency_map.end()) {
                    return (uint64_t)soft_constraint::max_score;
                }
                return (uint64_t)soft_constraint::max_score / (it->second + 1);
            };
        }

        ss::sstring name() const final {
            return fmt::format("equally spread on {}", _label_name);
        }

        ss::sstring _label_name;

        Mapper _mapper;
    };

    return soft_constraint(std::make_unique<impl>(
      std::move(label_name), std::forward<Mapper>(mapper)));
}

template<
  typename Mapper,
  typename T =
    typename std::invoke_result_t<Mapper, model::node_id>::value_type>
requires LabelMapper<Mapper, T> hard_constraint equally_spread_on_labels(
  ss::sstring label_name, std::vector<T> labels, Mapper&& mapper) {
    class impl : public hard_constraint::impl {
    public:
        impl(ss::sstring label, std::vector<T> labels, Mapper&& mapper)
          : _label_name(std::move(label))
          , _labels(std::move(labels))
          , _mapper(std::forward<Mapper>(mapper)) {}

        hard_constraint_evaluator
        make_evaluator(const replicas_t& current_replicas) const final {
            absl::flat_hash_map<T, size_t> frequency_map;

            for (const auto& l : _labels) {
                frequency_map[l] = 0;
            }

            for (auto& r : current_replicas) {
                auto const l = _mapper(r.node_id);
                if (!l) {
                    continue;
                }
                auto [it, _] = frequency_map.try_emplace(*l, 0);
                it->second += 1;
            }

            auto min_frequency = frequency_map.begin()->second;
            for (auto& [_, frequency] : frequency_map) {
                min_frequency = std::min(min_frequency, frequency);
            }

            return
              [this, min_frequency, frequency_map = std::move(frequency_map)](
                const allocation_node& candidate_node) {
                  auto node_label = _mapper(candidate_node.id());
                  if (!node_label) {
                      return false;
                  }
                  auto it = frequency_map.find(*node_label);
                  if (it != frequency_map.end()) {
                      return it->second == min_frequency;
                  }
                  return true;
              };
        }

        ss::sstring name() const final {
            return fmt::format(
              "equally spread on {} {} ", _labels, _label_name);
        }

        ss::sstring _label_name;
        std::vector<T> _labels;
        Mapper _mapper;
    };

    return hard_constraint(std::make_unique<impl>(
      std::move(label_name), std::move(labels), std::forward<Mapper>(mapper)));
}

template<typename Mapper, typename T>
requires LabelMapper<Mapper, T>
  hard_constraint exact_number_or_replicas_in_label(
    ss::sstring label_name, T label, int count, Mapper&& mapper) {
    class impl : public hard_constraint::impl {
    public:
        impl(ss::sstring label_name, T label, size_t count, Mapper&& mapper)
          : _label_name(std::move(label_name))
          , _label(std::move(label))
          , _count(count)
          , _mapper(std::forward<Mapper>(mapper)) {}

        hard_constraint_evaluator
        make_evaluator(const replicas_t& current_replicas) const final {
            auto replicas_on_requested_label = std::count_if(
              current_replicas.begin(),
              current_replicas.end(),
              [this](const model::broker_shard& replica) {
                  return _mapper(replica.node_id) == _label;
              });

            return [this, replicas_on_requested_label](
                     const allocation_node& candidate_node) {
                auto label = _mapper(candidate_node.id());
                if (replicas_on_requested_label < _count) {
                    return label == _label;
                }
                return label != _label;
            };
        }

        ss::sstring name() const final {
            return fmt::format(
              "exactly {} replicas in {} {}", _count, _label, _label_name);
        }

        ss::sstring _label_name;
        T _label;
        size_t _count;
        Mapper _mapper;
    };

    return hard_constraint(std::make_unique<impl>(
      std::move(label_name),
      std::move(label),
      count,
      std::forward<Mapper>(mapper)));
}
hard_constraint distinct_rack(const allocation_state& as);

inline soft_constraint distinct_rack_preferred(const allocation_state& state) {
    return make_soft_constraint(distinct_rack(state));
}

/**
 * This constraint prefers current replicas over the one that would require
 * partition move.
 */
soft_constraint prefer_current_replicas(
  const std::vector<model::broker_shard>& original_replica_set);

} // namespace cluster
