// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/partition_allocation_strategies.h"
#include "cluster/partition_allocator_new.h"
#include "model/metadata.h"
#include "raft/types.h"
#include "random/fast_prng.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>

#include <absl/container/node_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

struct partition_allocator_fixture {
    partition_allocator_fixture()
      : allocator(
        cluster::make_allocation_strategy<
          cluster::round_robin_allocation_strategy>(),
        cluster::make_reallocation_strategy<
          cluster::simple_decommissioned_deallocation_strategy>()) {}

    void register_node(int id, int core_count) {
        allocator.register_node(std::make_unique<cluster::allocation_node>(
          model::node_id(id),
          core_count,
          absl::node_hash_map<ss::sstring, ss::sstring>{}));
    }

    void saturate_all_machines() {
        auto total_capacity = std::accumulate(
          allocator.state().allocation_nodes().begin(),
          allocator.state().allocation_nodes().end(),
          0,
          [](int acc, const auto& node) {
              return acc + node.second->partition_capacity();
          });

        auto units = allocator.allocate(cluster::allocation_configuration{
          .replication_factor = 1,
          .partition_count = total_capacity,
        });
        BOOST_REQUIRE(units.has_value());
        for (auto pas : units.value().get_assignments()) {
            allocator.state().apply_update(pas.replicas, pas.group);
        }
    }

    uint allocated_nodes_count(
      const std::vector<cluster::partition_assignment>& allocs) {
        return std::accumulate(
          allocs.begin(),
          allocs.end(),
          0U,
          [](size_t acc, const cluster::partition_assignment& p_as) {
              return acc + p_as.replicas.size();
          });
    }

    bool all_nodes_empty() {
        return std::all_of(
          allocator.state().allocation_nodes().begin(),
          allocator.state().allocation_nodes().end(),
          [](const auto& n) { return n.second->empty(); });
    }

    int32_t max_capacity() {
        return std::accumulate(
          allocator.state().allocation_nodes().begin(),
          allocator.state().allocation_nodes().end(),
          0,
          [](int acc, auto& n) {
              return acc + n.second->partition_capacity();
          });
    }

    cluster::partition_allocator allocator;
    fast_prng prng;
};

FIXTURE_TEST(register_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    BOOST_REQUIRE(allocator.contains_node(model::node_id(0)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 3);
}

FIXTURE_TEST(unregister_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    allocator.unregister_node(model::node_id(1));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(0)));
    BOOST_REQUIRE(!allocator.contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 2);
}

FIXTURE_TEST(decommission_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    allocator.decommission_node(model::node_id(1));

    // only two of machines are available as one of them is decommissioned
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 2);
}

// FIXTURE_TEST(test_decommissioned_realloc, partition_allocator_tester) {
//     auto cfg = gen_topic_configuration(1, 3);
//     partition_allocator::allocation_units allocs = pa.allocate(cfg).value();

//     pa.decommission_node(model::node_id(2));
//     BOOST_REQUIRE_EQUAL(machines().size(), 3);
//     // only two of machines are available as one of them is decommissioned
//     BOOST_REQUIRE_EQUAL(available_machines().size(), 2);
//     auto assignment = *allocs.get_assignments().begin();

//     // reallocate
//     auto first_attempt = pa.reassign_decommissioned_replicas(assignment);
//     // first attempt should fail, there are not enough nodes to allocate
//     // replicas (requested rf = 3, while we have 2 nodes)
//     BOOST_REQUIRE_EQUAL(first_attempt.has_value(), false);

//     pa.register_node(std::make_unique<allocation_node>(
//       model::node_id(10), 8, std::unordered_map<ss::sstring,
//       ss::sstring>()));

//     auto second_attempt = pa.reassign_decommissioned_replicas(assignment);
//     //  second attempt should be successfull
//     BOOST_REQUIRE_EQUAL(second_attempt.has_value(), true);
//     BOOST_REQUIRE_EQUAL(second_attempt->get_assignments().size(), 1);
//     BOOST_REQUIRE_EQUAL(
//       second_attempt->get_assignments().begin()->replicas.size(), 3);
// }

// FIXTURE_TEST(
//   test_decommissioned_realloc_single_replica, partition_allocator_tester) {
//     auto cfg = gen_topic_configuration(1, 1);
//     partition_allocator::allocation_units allocs = pa.allocate(cfg).value();

//     pa.decommission_node(model::node_id(1));
//     BOOST_REQUIRE_EQUAL(machines().size(), 3);
//     // only two of machines are available as one of them is decommissioned
//     BOOST_REQUIRE_EQUAL(available_machines().size(), 2);
//     auto assignment = *allocs.get_assignments().begin();

//     // reallocate

//     auto reallocated = pa.reassign_decommissioned_replicas(assignment);
//     // first attempt should be successful as we have 2 nodes left and
//     requested
//     // rf = 1
//     BOOST_REQUIRE_EQUAL(reallocated.has_value(), true);
//     BOOST_REQUIRE_EQUAL(reallocated->get_assignments().size(), 1);
//     BOOST_REQUIRE_EQUAL(
//       reallocated->get_assignments().begin()->replicas.size(), 1);
// }

FIXTURE_TEST(invalid_allocation_over_capacity, partition_allocator_fixture) {
    register_node(0, 8);
    register_node(1, 4);
    register_node(2, 6);
    cluster::allocation_configuration cfg{
      .replication_factor = 1,
      .partition_count = 1,
    };

    saturate_all_machines();
    auto gr = allocator.state().last_group_id();
    BOOST_REQUIRE(allocator.allocate(cfg).has_error());
    // group id hasn't changed
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id(), gr);
}

FIXTURE_TEST(max_allocation, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    // available capacity
    // 3 * 7000 * 2 - 3*2 = 41994

    cluster::allocation_configuration cfg{
      .replication_factor = 3,
      .partition_count = max_capacity() / 3,
    };
    auto units = allocator.allocate(cfg).value();

    BOOST_REQUIRE_EQUAL(units.get_assignments().size(), 13998);
    BOOST_REQUIRE_EQUAL(allocated_nodes_count(units.get_assignments()), 41994);
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 13998);

    // make sure there is no room left after
    cluster::allocation_configuration single_partition_cfg{
      .replication_factor = 1,
      .partition_count = 1,
    };
    auto result = allocator.allocate(single_partition_cfg);
    BOOST_REQUIRE(result.has_error());
}

FIXTURE_TEST(unsatisfyable_diversity_assignment, partition_allocator_fixture) {
    register_node(0, 8);
    register_node(1, 4);
    register_node(2, 6);
    auto cfg = cluster::allocation_configuration{
      .replication_factor = 5,
      .partition_count = 1,
    };
    auto allocs = allocator.allocate(cfg);
    BOOST_REQUIRE(allocs.has_error());
    BOOST_REQUIRE_EQUAL(
      cluster::errc(allocs.error().value()),
      cluster::errc::topic_invalid_replication_factor);

    // ensure rollback happened
    BOOST_REQUIRE(all_nodes_empty());

    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 0);
}
FIXTURE_TEST(partial_assignment, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    auto max_partitions_in_cluster = max_capacity() / 3;
    cluster::allocation_configuration cfg_1{
      .replication_factor = 3,
      .partition_count = max_partitions_in_cluster - 1,
    };
    auto units_1 = allocator.allocate(cfg_1).value();
    BOOST_REQUIRE_EQUAL(
      units_1.get_assignments().size(), max_partitions_in_cluster - 1);

    // allocate 2 partitions - one should fail, returning null & deallocating
    cluster::allocation_configuration cfg_2{
      .replication_factor = 3, .partition_count = 2};

    auto units_2 = allocator.allocate(cfg_2);
    BOOST_REQUIRE(units_2.has_error());

    BOOST_REQUIRE_EQUAL(3, max_capacity());
    BOOST_REQUIRE_EQUAL(
      allocator.state().last_group_id()(), max_partitions_in_cluster - 1);
}
FIXTURE_TEST(max_deallocation, partition_allocator_fixture) {
    register_node(0, 3);
    register_node(1, 3);
    register_node(2, 3);
    // This test performs - 209994 partition assignments
    const auto max = max_capacity();

    auto cfg = cluster::allocation_configuration{
      .replication_factor = 3,
      .partition_count = max / 3,
    };
    {
        cluster::allocation_units allocs = allocator.allocate(cfg).value();

        BOOST_REQUIRE_EQUAL(allocs.get_assignments().size() * 3, max);

        BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), max / 3);
    }

    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), max / 3);
    BOOST_REQUIRE_EQUAL(max_capacity(), max);
}

FIXTURE_TEST(recovery_test, partition_allocator_fixture) {
    register_node(0, 3);
    register_node(1, 3);
    register_node(2, 3);
    auto create_topic_metadata = [this](int topics, int partitions) {
        std::vector<model::topic_metadata> ret;
        for (int t = 0; t < topics; t++) {
            model::topic_metadata t_md(model::topic_namespace(
              model::ns("default"), model::topic(ssx::sformat("topic_{}", t))));
            for (int p = 0; p < partitions; p++) {
                std::vector<model::broker_shard> replicas;
                for (int r = 0; r < 3; r++) {
                    replicas.push_back({model::node_id(r), prng() % 3});
                }
                model::partition_metadata p_md{model::partition_id(p)};
                p_md.replicas = std::move(replicas);
                p_md.leader_node = model::node_id(prng() % 3);
                t_md.partitions.push_back(std::move(p_md));
            }
            ret.push_back(std::move(t_md));
        }
        return ret;
    };
    // 100 topics with 12 partitions each replicated on 3 nodes each
    auto md = create_topic_metadata(100, 12);
    allocator.update_allocation_state(md, raft::group_id(0));
    // each node in the cluster holds one replica for each partition,
    // so it has to have topics * partitions shards allocated
    auto allocated_shards = 100 * 12;
    // Remaining capacity on node 0
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(0))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 1
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(1))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 2
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(2))
        ->second->allocated_partitions(),
      allocated_shards);
}

FIXTURE_TEST(allocation_units_test, partition_allocator_fixture) {
    register_node(1, 10);
    register_node(2, 11);
    register_node(3, 12);
    // just fill up the cluster partially
    auto cfg1 = cluster::allocation_configuration{
      .replication_factor = 3,
      .partition_count = 10,
    };
    {
        auto allocs = allocator.allocate(cfg1).value();
        BOOST_REQUIRE_EQUAL(allocs.get_assignments().size(), 10);
        BOOST_REQUIRE_EQUAL(
          allocated_nodes_count(allocs.get_assignments()), 3 * 10);
    }

    BOOST_REQUIRE(all_nodes_empty());

    // we do not decrement the highest raft group
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 10);
}
