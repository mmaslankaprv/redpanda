// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "reflection/arity.h"
#include "rpc/connection_cache.h"
#include "rpc/test/test_types.h"

#include <seastar/core/smp.hh>
#include <seastar/testing/thread_test_case.hh>

#include <fmt/core.h>

#include <vector>

SEASTAR_THREAD_TEST_CASE(serialize_pod) {
    auto b = iobuf();
    pod it;
    reflection::serialize(b, std::move(it));
    BOOST_CHECK_EQUAL(b.size_bytes(), pod_bytes());
}

SEASTAR_THREAD_TEST_CASE(verify_airty) {
    BOOST_CHECK_EQUAL(reflection::arity<pod>(), 3);
    BOOST_CHECK_EQUAL(reflection::arity<complex_custom>(), 2);
}

SEASTAR_THREAD_TEST_CASE(serialize_with_fragmented_buffer) {
    // Each field of a pod struct is serialized by it's type
    // So the total bytes written for a given type T is the sum:
    // sizeof(member1) + .... + sizeof(member n) + sizeof(blob)
    // where blob's size depends on the type serialized. For example
    // blob for ss::string is sizeof(int32_t) which denotes the number
    // of bytes to follow for the given string.
    auto b = iobuf();
    complex_custom it;
    it.oi.append(ss::temporary_buffer<char>(55));
    reflection::serialize(b, std::move(it));
    BOOST_CHECK_EQUAL(b.size_bytes(), 55 + complex_custom_bytes());
}

SEASTAR_THREAD_TEST_CASE(serialize_pod_with_vector) {
    auto b = iobuf();
    pod_with_vector it;
    reflection::serialize(b, std::move(it));
    BOOST_CHECK_EQUAL(b.size_bytes(), pod_with_vector_bytes());
}

SEASTAR_THREAD_TEST_CASE(serialize_pod_with_array) {
    auto b = iobuf();
    pod_with_array it;
    reflection::serialize(b, std::move(it));
    BOOST_CHECK_EQUAL(b.size_bytes(), pod_with_arr_bytes());
}

SEASTAR_THREAD_TEST_CASE(serialize_sstring_vector) {
    auto b = iobuf();
    test_rpc_header it;
    kv x;
    x.k = "foobar";
    x.v.append(ss::temporary_buffer<char>(87));
    it.hdrs.push_back(std::move(x));
    reflection::serialize(b, std::move(it));
    const size_t expected =
      /*
      struct kv {
         sstring k;              ---------------  sizeof(int32_t) + 6
         iobuf v; --------  sizeof(int32_t) + 87 bytes
      };
      struct test_rpc_header {
        int32_t size = 42;       ---------------- sizeof(int32_t)
        uint64_t checksum = 66;   ---------------- sizeof(int64_t)
        std::vector<kv> hdrs;    ---------------- sizeof(int32_t)
      };

      Total:  4 + 6 + 4 + 87 + 4 + 8 + 4 ........  117 bytes
      */
      117;
    BOOST_CHECK_EQUAL(b.size_bytes(), expected);
}

SEASTAR_THREAD_TEST_CASE(test_shards) {
    std::vector<model::node_id> cluster = {
      model::node_id(0), model::node_id(1), model::node_id(2)};
    ss::shard_id total_shards = 16;
    for (auto src : cluster) {
        for (auto n : cluster) {
            if (src == n) {
                continue;
            }
            std::set<ss::shard_id> shards;
            for (ss::shard_id i = 0; i < total_shards; ++i) {
                auto sh = rpc::connection_cache::shard_for(
                  src, i, n, total_shards);
                shards.insert(sh);
            }
            std::cout << fmt::format("{}->{} : [", src, n);
            for (auto s : shards) {
                std::cout << s << " ";
            }
            std::cout << "]" << std::endl;
        }
    }
}
