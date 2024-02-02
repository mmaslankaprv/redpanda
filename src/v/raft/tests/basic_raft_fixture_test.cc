// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "raft/errc.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "serde/serde.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>

#include <fmt/core.h>

#include <algorithm>

using namespace raft;

/**
 * Some basic Raft tests validating if Raft test fixture is working correctly
 */

TEST_F_CORO(raft_fixture, test_single_node_can_elect_leader) {
    auto& n0 = add_node(model::node_id(0), model::revision_id(0));
    co_await n0.init_and_start({n0.get_vnode()});
    auto leader = co_await wait_for_leader(10s);

    ASSERT_EQ_CORO(leader, model::node_id(0));
}

TEST_F_CORO(raft_fixture, test_multi_nodes_cluster_can_elect_leader) {
    co_await create_simple_group(5);

    auto leader = co_await wait_for_leader(10s);

    ASSERT_TRUE_CORO(all_ids().contains(leader));

    co_await tests::cooperative_spin_wait_with_timeout(10s, [this, leader] {
        for (const auto& [_, n] : nodes()) {
            if (leader != n->raft()->get_leader_id()) {
                return false;
            }
        }
        return true;
    });
}

TEST_F_CORO(raft_fixture, validate_replication) {
    co_await create_simple_group(5);

    auto leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());
    auto committed_offset = leader_node.raft()->committed_offset();

    // wait for committed offset to propagate
    co_await wait_for_committed_offset(committed_offset, 5s);
    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    co_await assert_logs_equal();
}

TEST_F_CORO(raft_fixture, validate_recovery) {
    co_await create_simple_group(5);
    auto leader = co_await wait_for_leader(10s);

    // stop one of the nodes
    co_await stop_node(model::node_id(3));

    leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    // replicate batches
    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());

    auto& new_n3 = add_node(model::node_id(3), model::revision_id(0));
    co_await new_n3.init_and_start(all_vnodes());

    // wait for committed offset to propagate
    auto committed_offset = leader_node.raft()->committed_offset();
    co_await wait_for_committed_offset(committed_offset, 5s);

    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    co_await assert_logs_equal();
}

TEST_F_CORO(raft_fixture, validate_adding_nodes_to_cluster) {
    co_await create_simple_group(1);
    // wait for leader
    auto leader = co_await wait_for_leader(10s);
    ASSERT_EQ_CORO(leader, model::node_id(0));
    auto& leader_node = node(leader);

    // replicate batches
    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());

    auto& n1 = add_node(model::node_id(1), model::revision_id(0));
    auto& n2 = add_node(model::node_id(2), model::revision_id(0));
    // start other two nodes with empty configuration
    co_await n1.init_and_start({});
    co_await n2.init_and_start({});

    // update group configuration
    co_await leader_node.raft()->replace_configuration(
      all_vnodes(), model::revision_id(0));

    // wait for committed offset to propagate
    auto committed_offset = leader_node.raft()->committed_offset();

    // wait for committed offset to propagate
    co_await wait_for_committed_offset(committed_offset, 10s);

    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    co_await assert_logs_equal();
}

TEST_F_CORO(
  raft_fixture, validate_committed_offset_advancement_after_log_flush) {
    co_await create_simple_group(3);
    // wait for leader
    auto leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    // replicate batches with acks=1 and validate that committed offset did not
    // advance
    auto committed_offset_before = leader_node.raft()->committed_offset();
    auto result = co_await leader_node.raft()->replicate(
      make_batches(10, 10, 128),
      replicate_options(consistency_level::leader_ack));

    ASSERT_TRUE_CORO(result.has_value());
    // wait for batches to be replicated on all of the nodes
    co_await tests::cooperative_spin_wait_with_timeout(
      10s, [this, expected = result.value().last_offset] {
          return std::all_of(
            nodes().begin(), nodes().end(), [expected](const auto& p) {
                return p.second->raft()->last_visible_index() == expected;
            });
      });
    ASSERT_EQ_CORO(
      committed_offset_before, leader_node.raft()->committed_offset());

    co_await assert_logs_equal();

    // flush log on all of the nodes
    co_await parallel_for_each_node(
      [](auto& n) { return n.raft()->maybe_flush_log(0); });
    co_await wait_for_committed_offset(result.value().last_offset, 10s);
}

TEST_F_CORO(raft_fixture, validate_relaxed_consistency) {
    co_await create_simple_group(3);
    // wait for leader
    co_await wait_for_leader(10s);

    bool stop = false;

    auto produce_fiber = ss::do_until(
      [&stop] { return stop; },
      [this] {
          ss::lw_shared_ptr<consensus> raft;
          for (auto& n : nodes()) {
              if (n.second->raft()->is_leader()) {
                  raft = n.second->raft();
                  break;
              }
          }

          if (!raft) {
              return ss::sleep(100ms);
          }
          return raft
            ->replicate(
              make_batches(10, 10, 128),
              replicate_options(consistency_level::leader_ack))
            .then([](result<replicate_result> result) {
                if (result.has_error()) {
                    // fmt::print(
                    //   "error(replicating): {}\n", result.error().message());
                }
            });
      });
    int transfers = 10;
    auto l_transfer_fiber = ss::do_until(
      [&transfers, &stop] { return transfers-- <= 0 || stop; },
      [this] {
          ss::lw_shared_ptr<consensus> raft;
          for (auto& n : nodes()) {
              if (n.second->raft()->is_leader()) {
                  raft = n.second->raft();
                  break;
              }
          }

          if (!raft) {
              return ss::sleep(100ms);
          }

          return raft
            ->transfer_leadership(
              transfer_leadership_request{.group = raft->group()})
            .then([](transfer_leadership_reply r) {
                if (r.result != raft::errc::success) {
                    fmt::print("error(transfering): {}\n", r);
                }
            })
            .then([] { return ss::sleep(200ms); });
      });

    co_await ss::sleep(30s);
    stop = true;
    co_await std::move(produce_fiber);
    co_await std::move(l_transfer_fiber);

    for (auto& n : nodes()) {
        auto r = n.second->raft();
        fmt::print(
          "leader: {} log_end: {}, visible: {} \n",
          r->is_leader(),
          r->dirty_offset(),
          r->last_visible_index());
        if (r->is_leader()) {
            for (auto& fs : r->get_follower_stats()) {
                fmt::print("f: {}\n", fs.second);
            }
        }
    }
}
