// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/stream_zstd.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/randoms.h"
#include "test_utils/rpc.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <vector>

struct checking_consumer {
    using batches_t = ss::circular_buffer<model::record_batch>;

    checking_consumer(ss::circular_buffer<model::record_batch> exp)
      : expected(std::move(exp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        auto current_batch = std::move(expected.front());
        expected.pop_front();
        BOOST_REQUIRE_EQUAL(current_batch.base_offset(), batch.base_offset());
        BOOST_REQUIRE_EQUAL(current_batch.last_offset(), batch.last_offset());
        BOOST_REQUIRE_EQUAL(current_batch.header().crc, batch.header().crc);
        BOOST_REQUIRE_EQUAL(current_batch.compressed(), batch.compressed());
        BOOST_REQUIRE_EQUAL(current_batch.header().type, batch.header().type);
        BOOST_REQUIRE_EQUAL(current_batch.size_bytes(), batch.size_bytes());
        BOOST_REQUIRE_EQUAL(current_batch.record_count(), batch.record_count());
        BOOST_REQUIRE_EQUAL(current_batch.term(), batch.term());
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    void end_of_stream() { BOOST_REQUIRE(expected.empty()); }

    batches_t expected;
};

SEASTAR_THREAD_TEST_CASE(append_entries_requests) {
    auto batches = storage::test::make_random_batches(
      model::offset(1), 3, false);

    for (auto& b : batches) {
        b.set_term(model::term_id(123));
    }

    auto rdr = model::make_memory_record_batch_reader(std::move(batches));
    auto readers = raft::details::share_n(std::move(rdr), 2).get0();
    auto meta = raft::protocol_metadata{
      .group = raft::group_id(1),
      .commit_index = model::offset(100),
      .term = model::term_id(10),
      .prev_log_index = model::offset(99),
      .prev_log_term = model::term_id(-1),
      .last_visible_index = model::offset(200),
    };
    raft::append_entries_request req(
      model::node_id(1), meta, std::move(readers.back()));

    readers.pop_back();
    auto d = async_serialize_roundtrip_rpc(std::move(req)).get0();

    BOOST_REQUIRE_EQUAL(d.node_id, model::node_id(1));
    BOOST_REQUIRE_EQUAL(d.meta.group, meta.group);
    BOOST_REQUIRE_EQUAL(d.meta.commit_index, meta.commit_index);
    BOOST_REQUIRE_EQUAL(d.meta.term, meta.term);
    BOOST_REQUIRE_EQUAL(d.meta.prev_log_index, meta.prev_log_index);
    BOOST_REQUIRE_EQUAL(d.meta.prev_log_term, meta.prev_log_term);
    BOOST_REQUIRE_EQUAL(d.meta.last_visible_index, meta.last_visible_index);

    auto batches_result = model::consume_reader_to_memory(
                            std::move(readers.back()), model::no_timeout)
                            .get0();
    d.batches
      .consume(checking_consumer(std::move(batches_result)), model::no_timeout)
      .get0();
}

model::broker create_test_broker() {
    return model::broker(
      model::node_id(random_generators::get_int(1000)), // id
      unresolved_address(
        "127.0.0.1",
        random_generators::get_int(10000, 20000)), // kafka api address
      unresolved_address(
        "127.0.0.1", random_generators::get_int(10000, 20000)), // rpc address
      "some_rack",
      model::broker_properties{
        .cores = 8 // cores
      });
}

SEASTAR_THREAD_TEST_CASE(heartbeat_request_roundtrip) {
    static constexpr int64_t one_k = 1'000;
    raft::heartbeat_request req;
    req.node_id = model::node_id(one_k);
    req.meta = std::vector<raft::protocol_metadata>(one_k);
    for (int64_t i = 0; i < one_k; ++i) {
        req.meta[i].group = raft::group_id(i);
        req.meta[i].commit_index = model::offset(i);
        req.meta[i].term = model::term_id(i);
        req.meta[i].prev_log_index = model::offset(i);
        req.meta[i].prev_log_term = model::term_id(i);
        req.meta[i].last_visible_index = model::offset(i);
    }
    iobuf buf;
    reflection::async_adl<raft::heartbeat_request>{}
      .to(buf, std::move(req))
      .get();
    BOOST_TEST_MESSAGE("Pre compression. Buffer size: " << buf);
    compression::stream_zstd codec;
    buf = codec.compress(std::move(buf));
    BOOST_TEST_MESSAGE("Post compression. Buffer size: " << buf);
    auto parser = iobuf_parser(codec.uncompress(std::move(buf)));
    auto res
      = reflection::async_adl<raft::heartbeat_request>{}.from(parser).get0();
    for (int64_t i = 0; i < one_k; ++i) {
        BOOST_REQUIRE_EQUAL(res.meta[i].group, raft::group_id(i));
        BOOST_REQUIRE_EQUAL(res.meta[i].commit_index, model::offset(i));
        BOOST_REQUIRE_EQUAL(res.meta[i].term, model::term_id(i));
        BOOST_REQUIRE_EQUAL(res.meta[i].prev_log_index, model::offset(i));
        BOOST_REQUIRE_EQUAL(res.meta[i].prev_log_term, model::term_id(i));
        BOOST_REQUIRE_EQUAL(res.meta[i].last_visible_index, model::offset(i));
    }
}
SEASTAR_THREAD_TEST_CASE(heartbeat_request_roundtrip_with_negative) {
    static constexpr int64_t one_k = 10;
    raft::heartbeat_request req;
    req.node_id = model::node_id(one_k);
    req.meta = std::vector<raft::protocol_metadata>(one_k);
    for (int64_t i = 0; i < one_k; ++i) {
        req.meta[i].group = raft::group_id(i);
        req.meta[i].commit_index = model::offset(-i - 100);
        req.meta[i].term = model::term_id(-i - 100);
        req.meta[i].prev_log_index = model::offset(-i - 100);
        req.meta[i].prev_log_term = model::term_id(-i - 100);
        req.meta[i].last_visible_index = model::offset(-i - 100);
    }
    iobuf buf;
    reflection::async_adl<raft::heartbeat_request>{}
      .to(buf, std::move(req))
      .get();
    BOOST_TEST_MESSAGE("Pre compression. Buffer size: " << buf);
    compression::stream_zstd codec;
    buf = codec.compress(std::move(buf));
    BOOST_TEST_MESSAGE("Post compression. Buffer size: " << buf);
    auto parser = iobuf_parser(codec.uncompress(std::move(buf)));
    auto res
      = reflection::async_adl<raft::heartbeat_request>{}.from(parser).get0();
    for (auto& m : res.meta) {
        BOOST_REQUIRE_EQUAL(m.commit_index, model::offset{});
        BOOST_REQUIRE_EQUAL(m.term, model::term_id{-1});
        BOOST_REQUIRE_EQUAL(m.prev_log_index, model::offset{});
        BOOST_REQUIRE_EQUAL(m.prev_log_term, model::term_id{});
        BOOST_REQUIRE_EQUAL(m.last_visible_index, model::offset{});
    }
}
SEASTAR_THREAD_TEST_CASE(heartbeat_response_roundtrip) {
    static constexpr int64_t group_count = 10000;
    raft::heartbeat_reply reply;
    reply.meta.reserve(group_count);

    for (size_t i = 0; i < group_count; ++i) {
        auto commited_idx = model::offset(
          random_generators::get_int(-1, 1000000000));
        auto dirty_idx = commited_idx
                         + model::offset(random_generators::get_int(10000));

        reply.meta.push_back(raft::append_entries_reply{
          .node_id = model::node_id(1),
          .group = raft::group_id(i),
          .term = model::term_id(random_generators::get_int(-1, 1000)),
          .last_committed_log_index = commited_idx,
          .last_dirty_log_index = dirty_idx,
          .result = raft::append_entries_reply::status::success});
    }
    absl::flat_hash_map<raft::group_id, raft::append_entries_reply> expected;
    expected.reserve(reply.meta.size());
    for (const auto& m : reply.meta) {
        expected.emplace(m.group, m);
    }
    iobuf buf;
    reflection::async_adl<raft::heartbeat_reply>{}
      .to(buf, std::move(reply))
      .get();
    BOOST_TEST_MESSAGE("Pre compression. Buffer size: " << buf);
    compression::stream_zstd codec;
    buf = codec.compress(std::move(buf));
    BOOST_TEST_MESSAGE("Post compression. Buffer size: " << buf);
    auto parser = iobuf_parser(codec.uncompress(std::move(buf)));
    auto result
      = reflection::async_adl<raft::heartbeat_reply>{}.from(parser).get0();

    for (size_t i = 0; i < result.meta.size(); ++i) {
        auto gr = result.meta[i].group;
        BOOST_REQUIRE_EQUAL(expected[gr].node_id, result.meta[i].node_id);
        BOOST_REQUIRE_EQUAL(expected[gr].group, result.meta[i].group);
        BOOST_REQUIRE_EQUAL(expected[gr].term, result.meta[i].term);
        BOOST_REQUIRE_EQUAL(
          expected[gr].last_committed_log_index,
          result.meta[i].last_committed_log_index);
        BOOST_REQUIRE_EQUAL(
          expected[gr].last_dirty_log_index,
          result.meta[i].last_dirty_log_index);
        BOOST_REQUIRE_EQUAL(expected[gr].result, result.meta[i].result);
    }
}

SEASTAR_THREAD_TEST_CASE(snapshot_metadata_roundtrip) {
    auto n1 = tests::random_broker(0, 100);
    auto n2 = tests::random_broker(0, 100);
    auto n3 = tests::random_broker(0, 100);
    std::vector<model::broker> nodes{n1, n2, n3};
    raft::group_nodes current{
      .voters = {n1.id(), n3.id()}, .learners = {n2.id()}};

    raft::group_configuration cfg(nodes, current, model::revision_id(0));

    auto ct = ss::lowres_clock::now();
    raft::snapshot_metadata metadata{
      .last_included_index = model::offset(123),
      .last_included_term = model::term_id(32),
      .latest_configuration = cfg,
      .cluster_time = ct,
    };

    auto d = serialize_roundtrip_rpc(std::move(metadata));

    BOOST_REQUIRE_EQUAL(d.last_included_index, model::offset(123));
    BOOST_REQUIRE_EQUAL(d.last_included_term, model::term_id(32));
    BOOST_REQUIRE(d.cluster_time == ct);
    BOOST_REQUIRE_EQUAL(d.latest_configuration, cfg);
}

SEASTAR_THREAD_TEST_CASE(vote_request_backward_compatibility_test) {
    raft::vote_request current_version;
    current_version.node_id = model::node_id(20);
    current_version.group = raft::group_id(1024);
    current_version.term = model::term_id(2048);
    current_version.prev_log_index = model::offset(4096);
    current_version.prev_log_term = model::term_id(8192);
    current_version.leadership_transfer = true;
    current_version.configuration_revision = model::revision_id(512);

    struct old_version {
        model::node_id node_id;
        raft::group_id group;
        model::term_id term;
        model::offset prev_log_index;
        model::term_id prev_log_term;
        bool leadership_transfer;
    };

    BOOST_TEST_MESSAGE("current version -> current version");
    // # 1 current version -> current version
    auto curr = current_version;
    auto d_current_version = serialize_roundtrip_rpc(std::move(curr));
    BOOST_REQUIRE_EQUAL(current_version.node_id, d_current_version.node_id);
    BOOST_REQUIRE_EQUAL(current_version.term, d_current_version.term);
    BOOST_REQUIRE_EQUAL(
      current_version.prev_log_index, d_current_version.prev_log_index);
    BOOST_REQUIRE_EQUAL(
      current_version.leadership_transfer,
      d_current_version.leadership_transfer);
    BOOST_REQUIRE_EQUAL(
      current_version.configuration_revision,
      d_current_version.configuration_revision);

    BOOST_TEST_MESSAGE("current version -> old version");
    // # 2 current version -> old version
    curr = current_version;
    iobuf buf;
    reflection::adl<raft::vote_request>{}.to(buf, std::move(curr));
    iobuf_parser p(std::move(buf));
    auto d_old = reflection::adl<old_version>{}.from(p);

    BOOST_REQUIRE_EQUAL(current_version.node_id, d_old.node_id);
    BOOST_REQUIRE_EQUAL(current_version.term, d_old.term);
    BOOST_REQUIRE_EQUAL(current_version.prev_log_index, d_old.prev_log_index);
    BOOST_REQUIRE_EQUAL(
      current_version.leadership_transfer, d_old.leadership_transfer);

    BOOST_TEST_MESSAGE("old version -> current version");
    // # 3 old version -> current version
    iobuf buf_2;
    reflection::adl<old_version>{}.to(buf_2, std::move(d_old));
    iobuf_parser p_2(std::move(buf_2));
    auto d_current_from_old = reflection::adl<raft::vote_request>{}.from(p_2);

    BOOST_REQUIRE_EQUAL(current_version.node_id, d_current_from_old.node_id);
    BOOST_REQUIRE_EQUAL(current_version.term, d_current_from_old.term);
    BOOST_REQUIRE_EQUAL(
      current_version.prev_log_index, d_current_from_old.prev_log_index);
    BOOST_REQUIRE_EQUAL(
      current_version.leadership_transfer,
      d_current_from_old.leadership_transfer);

    BOOST_REQUIRE_EQUAL(
      d_current_from_old.configuration_revision, model::revision_id{});
}
