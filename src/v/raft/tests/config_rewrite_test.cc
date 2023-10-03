
// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "raft/heartbeats.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/types.h"
#include "test_utils/randoms.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <optional>
#include <utility>
#include <vector>

using namespace std::chrono_literals;

static ss::sstring data_dir = "/home/mmaslanka/dev/support/dw";
SEASTAR_THREAD_TEST_CASE(rewrite_raft_configuration) {
    ss::sharded<features::feature_table> ft;
    storage::api st_api(
      []() {
          return storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            data_dir,
            storage::make_sanitized_file_config());
      },
      []() {
          return storage::log_config(
            data_dir,
            100_MiB,
            ss::default_priority_class(),
            storage::make_sanitized_file_config());
      },
      ft);
    st_api.start().get();
    model::revision_id target_rev(28);
    for (int i = 0; i < 32; ++i) {
        auto src_log = st_api.log_mgr()
                         .manage(storage::ntp_config(
                           model::ntp(
                             model::kafka_namespace,
                             model::topic("schemas"),
                             model::partition_id(i)),
                           data_dir,
                           nullptr,
                           model::revision_id(28)))
                         .get();
        auto target_log = st_api.log_mgr()
                            .manage(storage::ntp_config(
                              model::ntp(
                                model::kafka_namespace,
                                model::topic("__consumer_offsets"),
                                model::partition_id(i)),
                              data_dir,
                              nullptr,
                              target_rev))
                            .get();
        auto reader = src_log
                        ->make_reader(storage::log_reader_config(
                          model::offset(0),
                          model::offset::max(),
                          ss::default_priority_class()))
                        .get();

        auto batches = model::consume_reader_to_memory(
                         std::move(reader), model::no_timeout)
                         .get();
        ss::circular_buffer<model::record_batch> transformed;

        raft::group_configuration new_cfg(
          {raft::vnode(model::node_id{0}, model::revision_id(28)),
           raft::vnode(model::node_id{1}, model::revision_id(28)),
           raft::vnode(model::node_id{2}, model::revision_id(28))},
          target_rev);

        for (auto& b : batches) {
            if (
              b.header().type == model::record_batch_type::raft_configuration) {
                auto records = b.copy_records();
                auto cfg = reflection::from_iobuf<raft::group_configuration>(
                  records.front().release_value());
                fmt::print("config: {}\n", cfg);
                auto cfg_batches
                  = raft::details::serialize_configuration_as_batches(new_cfg);
                cfg_batches.begin()->set_term(b.term());
                transformed.push_back(std::move(*cfg_batches.begin()));
            } else {
                transformed.push_back(std::move(b));
            }
        }

        auto tr_reader = model::make_memory_record_batch_reader(
          std::move(transformed));

        auto appender = target_log->make_appender(storage::log_append_config{
          .should_fsync = storage::log_append_config::fsync::yes,
          .io_priority = ss::default_priority_class(),
        });
        tr_reader.for_each_ref(std::move(appender), model::no_timeout).get();
        target_log->flush().get();

        st_api.log_mgr().shutdown(src_log->config().ntp()).get();
        st_api.log_mgr().shutdown(target_log->config().ntp()).get();
    }
    st_api.stop().get();
}
