
#include "config/property.h"
#include "features/feature_table.h"
#include "finjector/stress_fiber.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/offset_translator_state.h"
#include "test_utils/test.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/file.hh>

#include <cstdint>
#include <optional>
#include <string_view>

using namespace std::chrono_literals;

std::string_view dir
  = "/home/mmaslanka/dev/support/delta-inconsistency/test_data";

storage::kvstore_config kv_cfg() {
    return {10_MiB, config::mock_binding(10ms), ss::sstring(dir), std::nullopt};
}

storage::log_config log_mgr_cfg() { return {ss::sstring(dir), 1_MiB}; }

struct printing_consumer {
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        fmt::print("batch: {}\n", batch.header());
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}
};

struct collecting_consumer {
    chunked_vector<model::record_batch_header> headers;
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        if (
          batch.header().type == model::record_batch_type::archival_metadata) {
            headers.push_back(batch.header());
        }
        co_return ss::stop_iteration::no;
    }

    chunked_vector<model::record_batch_header> end_of_stream() {
        return std::move(headers);
    }
};

// TEST_CORO(kv_override, override_configuration) {
//     ss::sharded<features::feature_table> ft;
//     config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
//     co_await ft.start();
//     storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
//     co_await storage.start();
//     // auto& kvstore = storage.kvs();
//     model::ntp cg_0(
//       model::kafka_namespace, model::kafka_consumer_offsets_topic, 2);
//     // model::ntp cg_1(model::kafka_namespace, model::topic("test"), 1);
//     // model::ntp cg_2(model::kafka_namespace, model::topic("test"), 2);
//     storage::ntp_config cfg_0(cg_0, ss::sstring(dir));
//     storage::ntp_config::default_overrides o;
//     o.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
//     cfg_0.set_overrides(o);
//     // storage::ntp_config cfg_1(cg_1, ss::sstring(dir));
//     // storage::ntp_config cfg_2(cg_2, ss::sstring(dir));

//     auto log_0 = co_await storage.log_mgr().manage(
//       std::move(cfg_0),
//       raft::group_id(0),
//       model::offset_translator_batch_types());

//     // auto log_1 = co_await storage.log_mgr().manage(
//     //   std::move(cfg_1),
//     //   raft::group_id(2),
//     //   model::offset_translator_batch_types());

//     // auto log_2 = co_await storage.log_mgr().manage(
//     //   std::move(cfg_2),
//     //   raft::group_id(2),
//     //   model::offset_translator_batch_types());

//     fmt::print("ntp: {}", cg_0);
//     fmt::print("offsets: {}\n", log_0->offsets());
//     ss::abort_source as;

//     // std::unique_ptr<storage::hash_key_offset_map> compaction_hash_key_map
//     //   = std::make_unique<storage::hash_key_offset_map>();
//     // co_await compaction_hash_key_map->initialize(64 * 1024 * 1024);
//     // for (int i = 0; i < 50000; ++i) {
//     //     co_await log_0->housekeeping(storage::housekeeping_config(
//     //       model::timestamp::max(),
//     //       std::nullopt,
//     //       model::offset::max(),
//     //       std::nullopt,
//     //       ss::default_priority_class(),
//     //       as,
//     //       std::nullopt,
//     //       compaction_hash_key_map.get()));
//     // }
//     {
//         auto reader_0 = co_await
//         log_0->make_reader(storage::log_reader_config(
//           log_0->offsets().start_offset,
//           log_0->offsets().committed_offset,
//           ss::default_priority_class()));

//         co_await reader_0.for_each_ref(printing_consumer{},
//         model::no_timeout);
//     }
//     co_await storage.stop();
//     co_await ft.stop();
// }
using log_ptr = ss::shared_ptr<storage::log>;
ss::future<fragmented_vector<model::record_batch>>
read_log(log_ptr log, model::offset next_to_read) {
    storage::log_reader_config config(
      next_to_read,
      model::offset::max(),
      0,
      1024 * 1024,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    config.fill_gaps = true;
    auto reader = co_await log->make_reader(config);

    co_return co_await model::consume_reader_to_fragmented_memory(
      std::move(reader), model::no_timeout);
}

ss::future<> read_in_ranges(log_ptr log) {
    model::offset next_to_read = log->offsets().start_offset;

    while (next_to_read < log->offsets().committed_offset) {
        auto batches = co_await read_log(log, next_to_read);
        next_to_read = model::next_offset(batches.back().last_offset());
        for (auto& batch : batches) {
            if (
              batch.header().type != model::record_batch_type::raft_data
              && batch.header().type != model::record_batch_type::ghost_batch) {
                fmt::print("batch: {}\n", batch.header());
            }
        }
    }

    co_return;
}

ss::future<chunked_vector<model::record_batch_header>>
read_archival_batches(log_ptr log) {
    chunked_vector<model::record_batch_header> headers;
    model::offset next_to_read = log->offsets().start_offset;

    while (next_to_read < log->offsets().committed_offset) {
        auto batches = co_await read_log(log, next_to_read);
        next_to_read = model::next_offset(batches.back().last_offset());
        for (auto& batch : batches) {
            if (
              batch.header().type
              == model::record_batch_type::archival_metadata) {
                // fmt::print("batch: {}\n", batch.header());
                headers.push_back(batch.header());
            }
        }
    }
    co_return headers;
}

ss::future<chunked_vector<model::record_batch_header>>
read_archival_batches_reader(log_ptr log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::offset::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    auto reader = co_await log->make_reader(config);
    co_return co_await reader.for_each_ref(
      collecting_consumer{}, model::no_timeout);
}

ss::future<> copy_in_ranges(log_ptr source, log_ptr target) {
    model::offset next_to_read = source->offsets().start_offset;

    while (next_to_read < source->offsets().committed_offset) {
        auto batches = co_await read_log(source, next_to_read);
        auto last_read = batches.back().last_offset();
        next_to_read = model::next_offset(batches.back().last_offset());
        auto appender = target->make_appender(storage::log_append_config{
          .should_fsync = storage::log_append_config::fsync::no,
          .io_priority = ss::default_priority_class(),
          .timeout = model::no_timeout});
        auto ar = co_await model::make_fragmented_memory_record_batch_reader(
                    std::move(batches))
                    .for_each_ref(std::move(appender), model::no_timeout);
        vassert(ar.last_offset == last_read, "last offset mismatch");
    }
    co_return;
}

model::record_batch make_archival_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::archival_metadata, model::offset(0));
    for (int i = 0; i < random_generators::get_int(1, 10); ++i) {
        builder.add_raw_kv(
          iobuf::from("archival-key"), iobuf::from("archival-value"));
    }
    return std::move(builder).build();
}

model::record_batch make_raft_cfg() {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_configuration, model::offset(0));

    builder.add_raw_kv(iobuf::from("r-key"), iobuf::from("r-value"));

    return std::move(builder).build();
}

iobuf key_buffer() {
    return iobuf::from(fmt::format("key-{}", random_generators::get_int(0, 3)));
}

model::record_batch make_data_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    for (auto i : boost::irange(random_generators::get_int(100, 500))) {
        builder.add_raw_kv(
          key_buffer(), iobuf::from(fmt::format("data-value-{}", i)));
    }

    return std::move(builder).build();
}

static const float archival_frequency = 0.01;
struct producer {
    std::tuple<model::record_batch_reader, bool> generate_batches() {
        chunked_vector<model::record_batch> batches;
        bool archival = false;
        if (
          random_generators::get_int(0, 100) <= archival_frequency * 100
          && segments % 4 == 0) {
            batches.push_back(make_archival_batch());
            archival = true;
        } else if (random_generators::get_int(0, 1000) == 0) {
            batches.push_back(make_raft_cfg());
        } else {
            batches.push_back(make_data_batch());
        }

        return std::make_tuple(
          model::make_fragmented_memory_record_batch_reader(std::move(batches)),
          archival);
    }

    ss::future<> produce_data(log_ptr log, size_t segment_count) {
        while (archival_batch_offsets.size() < 1000) {
            auto appender = log->make_appender(storage::log_append_config{
              .should_fsync = storage::log_append_config::fsync::no,
              .io_priority = ss::default_priority_class(),
              .timeout = model::no_timeout});

            auto [reader, archival_batch] = generate_batches();
            auto ar = co_await reader.for_each_ref(
              std::move(appender), model::no_timeout);
            if (archival_batch) {
                archival_batch_offsets.push_back(ar.base_offset);
            }
            segments = log->segment_count();
        }
        co_await log->flush();
    }
    size_t segments = 0;
    chunked_vector<model::offset> archival_batch_offsets;
};
storage::ntp_config make_ntp_config(const model::ntp& ntp) {
    storage::ntp_config test_ntp_cfg(ntp, ss::sstring(dir));
    storage::ntp_config::default_overrides o;
    o.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    o.cache_enabled = storage::with_cache::no;
    o.segment_size = 10_MiB;
    o.tombstone_retention_ms = tristate<std::chrono::milliseconds>(1s);
    test_ntp_cfg.set_overrides(o);

    return test_ntp_cfg;
}

// TEST_CORO(compacted_reads, read_compacted_topic) {
//     ss::sharded<features::feature_table> ft;
//     config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
//     co_await ft.start();
//     storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
//     co_await storage.start();
//     // auto& kvstore = storage.kvs();
//     model::partition_id partition{0};
//     model::ntp test_ntp(
//       model::kafka_namespace, model::kafka_consumer_offsets_topic,
//       partition);
//     model::ntp target_ntp(model::kafka_namespace, "test_copy", partition);

//     storage::ntp_config src_ntp_cfg = make_ntp_config(test_ntp);

//     auto log = co_await storage.log_mgr().manage(
//       make_ntp_config(test_ntp),
//       raft::group_id(0),
//       model::offset_translator_batch_types());
//     co_await log->start(std::nullopt);
//     auto target_log = co_await storage.log_mgr().manage(
//       make_ntp_config(target_ntp),
//       raft::group_id(0),
//       model::offset_translator_batch_types());
//     co_await target_log->start(std::nullopt);

//     fmt::print("ntp: {}\n", test_ntp);
//     fmt::print("offsets: {}\n", log->offsets());
//     ss::abort_source as;

//     co_await copy_in_ranges(log, target_log);

//     co_await read_in_ranges(target_log);

//     co_await storage.stop();
//     co_await ft.stop();
// }

TEST(compacted_reads, read_compacted_topic) {
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);
    stress_fiber_manager stress_mgr;
    stress_config stress_cfg;
    stress_cfg.min_spins_per_scheduling_point = 1;
    stress_cfg.max_spins_per_scheduling_point = 100;
    stress_cfg.num_fibers = 5;
    // stress_mgr.start(stress_cfg);
    ss::sharded<features::feature_table> ft;
    config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
    ft.start().get();
    ft.local().testing_activate_all();
    storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
    storage.start().get();
    // auto& kvstore = storage.kvs();
    model::partition_id partition{2};

    model::ntp compacted_ntp(
      model::kafka_namespace, model::kafka_consumer_offsets_topic, partition);

    auto log = storage.log_mgr()
                 .manage(
                   make_ntp_config(compacted_ntp),
                   raft::group_id(0),
                   model::offset_translator_batch_types())
                 .get();
    log->start(std::nullopt).get();

    fmt::print("ntp: {}\n", compacted_ntp);
    ss::abort_source as;
    producer p;
    bool stop = false;
    auto produce_fiber = p.produce_data(log, 100).then([&] {
        return ss::do_until(
                 [&] { return log->segment_count() <= 2; },
                 [&] {
                     fmt::print(
                       "waiting for compaction segments: {}\n",
                       log->segment_count());
                     return ss::sleep(1s);
                 })
          .then([] { return ss::sleep(5s); })
          .then([&] { stop = true; });
    });

    std::unique_ptr<storage::hash_key_offset_map> compaction_hash_key_map
      = std::make_unique<storage::hash_key_offset_map>();
    compaction_hash_key_map->initialize(4).get();

    auto compact = [&] {
        return ss::do_until(
          [&] { return stop; },
          [&] {
              return log->housekeeping(storage::housekeeping_config(
                model::timestamp::max(),
                std::nullopt,
                model::offset::max(),
                std::nullopt,
                ss::default_priority_class(),
                as,
                std::nullopt,
                compaction_hash_key_map.get()));
          });
    };
    auto compact_fiber = compact();

    produce_fiber.get();
    compact_fiber.get();

    auto archival_after = read_archival_batches_reader(log).get();
    fmt::print("number of batches: {}\n", p.archival_batch_offsets.size());

    ASSERT_EQ(archival_after.size(), p.archival_batch_offsets.size());
    {
        auto reader_0 = log
                          ->make_reader(storage::log_reader_config(
                            log->offsets().start_offset,
                            log->offsets().committed_offset,
                            ss::default_priority_class()))
                          .get();

        reader_0.for_each_ref(printing_consumer{}, model::no_timeout).get();
    }
    storage.stop().get();
    ft.stop().get();
    for (size_t i = 0; i < p.archival_batch_offsets.size(); ++i) {
        if (p.archival_batch_offsets[i] != archival_after[i].base_offset) {
            fmt::print(">>> before-batch: {}\n", p.archival_batch_offsets[i]);
            fmt::print(">>> after-batch: {}\n", archival_after[i]);
        }
    }

    // stress_mgr.stop().get();
}
