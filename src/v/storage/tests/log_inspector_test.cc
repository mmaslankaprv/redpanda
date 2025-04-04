
#include "config/property.h"
#include "kafka/server/group_data_parser.h"
#include "kafka/server/group_recovery_consumer.h"
#include "model/fundamental.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/offset_translator_state.h"
#include "test_utils/test.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/file.hh>

#include <absl/strings/str_split.h>

#include <cstdint>
#include <optional>
#include <string_view>

using namespace std::chrono_literals;

struct printing_consumer {
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        fmt::print("batch: {}\n", batch.header());
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}
};

static auto serializer = kafka::make_consumer_offsets_serializer();
void handle_record(model::record r) {
    try {
        auto record_type = serializer.get_metadata_type(r.key().copy());
        switch (record_type) {
        case kafka::offset_commit: {
            auto v = serializer.decode_offset_metadata(std::move(r));
            fmt::print(
              "offset_commit: {}/{} offset: {}\n",
              v.key.topic,
              v.key.partition,
              v.value->offset);
            return;
        }
        case kafka::group_metadata:
            serializer.decode_group_metadata(std::move(r));
            return;
        case kafka::noop:
            // ignore noops, they are handled for backward compatibility
            return;
        }
        __builtin_unreachable();
    } catch (...) {
    }
}

struct co_printing_consumer {
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        fmt::print("batch: {}\n", batch.header());
        batch.for_each_record(
          [&](model::record r) { handle_record(std::move(r)); });
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

using log_ptr = ss::shared_ptr<storage::log>;
ss::future<fragmented_vector<model::record_batch>> read_kafka_log(log_ptr log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::offset::max(),
      0,
      100 * 1024 * 1024,
      ss::default_priority_class(),
      {model::record_batch_type::raft_data},
      std::nullopt,
      std::nullopt);
    config.fill_gaps = false;
    auto reader = co_await log->make_reader(config);

    co_return co_await model::consume_reader_to_fragmented_memory(
      std::move(reader), model::no_timeout);
}

ss::future<> print_kafka_log(log_ptr log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::offset::max(),
      0,
      100 * 1024 * 1024,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    config.fill_gaps = false;
    auto reader = co_await log->make_reader(config);
    co_await reader.for_each_ref(co_printing_consumer{}, model::no_timeout);
}

using log_ptr = ss::shared_ptr<storage::log>;
ss::future<fragmented_vector<model::record_batch>>
read_log(log_ptr log, model::offset next_to_read) {
    storage::log_reader_config config(
      next_to_read,
      model::offset::max(),
      0,
      random_generators::get_int(1, 512 * 1024),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    config.fill_gaps = true;
    auto reader = co_await log->make_reader(config);

    co_return co_await model::consume_reader_to_fragmented_memory(
      std::move(reader), model::no_timeout);
}

using headers_t = chunked_vector<model::record_batch_header>;

struct header_collecting_consumer {
    ss::future<ss::stop_iteration>
    operator()(const model::record_batch& batch) {
        headers.push_back(batch.header());

        co_return ss::stop_iteration::no;
    }

    headers_t end_of_stream() { return std::move(headers); }
    headers_t headers;
};

ss::future<headers_t> read_in_one_pass(log_ptr log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::offset::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    config.fill_gaps = true;
    auto reader = co_await log->make_reader(config);
    co_return co_await reader.for_each_ref(
      header_collecting_consumer{}, model::no_timeout);
}

ss::future<headers_t> read_in_ranges(log_ptr log) {
    model::offset next_to_read = log->offsets().start_offset;
    headers_t headers;
    while (next_to_read < log->offsets().committed_offset) {
        auto batches = co_await read_log(log, next_to_read);
        next_to_read = model::next_offset(batches.back().last_offset());
        for (auto& batch : batches) {
            fmt::print("batch: {}\n", batch.header());
            headers.push_back(batch.header());
        }
    }

    co_return headers;
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

    ss::future<> produce_data(log_ptr log, size_t) {
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

// TEST(compacted_reads, read_compacted_topic) {
//     ss::sharded<features::feature_table> ft;
//     config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
//     ft.start().get();
//     ft.local().testing_activate_all();
//     storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
//     storage.start().get();

//     model::partition_id partition{14};

//     model::ntp compacted_ntp(
//       model::kafka_namespace, model::kafka_consumer_offsets_topic,
//       partition);

//     auto log = storage.log_mgr()
//                  .manage(
//                    make_ntp_config(compacted_ntp,
//                    model::revision_id(23910067)), raft::group_id(0),
//                    model::offset_translator_batch_types())
//                  .get();
//     log->start(std::nullopt).get();
//     kafka::group_recovery_consumer consumer(
//       log, model::no_timeout, ss::default_priority_class());

//     storage.stop().get();
// }

// TEST(compacted_reads, read_compacted_topic) {
//     ss::sharded<features::feature_table> ft;
//     config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
//     ft.start().get();
//     ft.local().testing_activate_all();
//     storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
//     storage.start().get();

//     model::partition_id partition{14};

//     model::ntp compacted_ntp(
//       model::kafka_namespace, model::kafka_consumer_offsets_topic,
//       partition);

//     auto log = storage.log_mgr()
//                  .manage(
//                    make_ntp_config(compacted_ntp,
//                    model::revision_id(23880324)), raft::group_id(0),
//                    model::offset_translator_batch_types())
//                  .get();
//     log->start(std::nullopt).get();
//     print_kafka_log(log).get();

//     ss::abort_source as;
//     for (auto i = 0; i < 50; ++i) {
//         log
//           ->housekeeping(storage::housekeeping_config(
//             model::timestamp::max(),
//             std::nullopt,
//             model::offset::max(),
//             std::nullopt,
//             ss::default_priority_class(),
//             as,
//             std::nullopt))
//           .get();
//     }
//     fmt::print(">>> COMPACTED LOGS\n");
//     print_kafka_log(log).get();

//     storage.stop().get();
//     ft.stop().get();
//     ASSERT_TRUE(false);
// }

struct storage_stack {
    ss::future<> start(const ss::sstring& dir) {
        auto log_cfg_provider = [dir] {
            return storage::log_config{ss::sstring(dir), 1_MiB};
        };
        auto kv_cfg_provider = [dir] {
            return storage::kvstore_config{
              10_MiB,
              config::mock_binding(10ms),
              ss::sstring(dir),
              std::nullopt};
        };
        ;

        config::shard_local_cfg().log_compaction_use_sliding_window.set_value(
          true);
        co_await ft.start();
        ft.local().testing_activate_all();
        storage = std::make_unique<storage::api>(
          kv_cfg_provider, log_cfg_provider, ft);
        co_await storage->start();
    }
    ss::future<> stop() {
        co_await storage->stop();
        co_await ft.stop();
    }
    ss::sharded<features::feature_table> ft;
    std::unique_ptr<storage::api> storage;
};
std::string_view dir
  = "/home/mmaslanka/dev/support/wunderkind/n0/var/lib/redpanda/data";

storage::ntp_config make_ntp_config(
  const model::ntp& ntp,
  const ss::sstring& dir,
  model::revision_id rev = model::revision_id(0)) {
    storage::ntp_config test_ntp_cfg(ntp, dir, nullptr, rev);
    storage::ntp_config::default_overrides o;
    o.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;

    o.cache_enabled = storage::with_cache::no;
    o.segment_size = 1_MiB;
    o.compaction_strategy test_ntp_cfg.set_overrides(o);

    return test_ntp_cfg;
}

model::revision_id
find_revision_id(const model::ntp& ntp, const std::filesystem::path& dir) {
    auto path = std::filesystem::path(dir) / std::filesystem::path(ntp.ns())
                / std::filesystem::path(ntp.tp.topic());
    for (auto& dir_entry : std::filesystem::directory_iterator(path)) {
        if (dir_entry.path().filename().string().starts_with(
              fmt::to_string(ntp.tp.partition))) {
            std::vector<std::string> name_parts = absl::StrSplit(
              dir_entry.path().filename().string(), '_');
            return model::revision_id(std::atoi(name_parts[1].c_str()));
        }
    }
    return model::revision_id(0);
}

ss::future<model::record_batch_reader>
make_reader(ss::shared_ptr<storage::log> log) {
    storage::log_reader_config config(
      log->offsets().start_offset,
      model::offset::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    return log->make_reader(config);
}

// TEST(compacted_reads, compare_read_methods) {
//     ss::abort_source as;
//     chunked_hash_map<
//       int,
//       chunked_hash_map<int, kafka::group_recovery_consumer_state>>
//       groups;
//     for (int pod = 0; pod < 6; ++pod) {
//         storage_stack s_stack;
//         auto dir = fmt::format(
//           "/home/mmaslanka/dev/support/wunderkind-data/pod-{}", pod);
//         s_stack.start(ss::sstring(dir)).get();

//         // for (int part = 0; part < 16; ++part) {
//         auto part = 12;
//         model::ntp ntp(
//           model::kafka_namespace,
//           model::kafka_consumer_offsets_topic,
//           model::partition_id(part));

//         auto revision = find_revision_id(ntp, dir);

//         if (revision != model::revision_id(0)) {
//             fmt::print(
//               ">>> checking pod: {} ntp: {} rev: {}\n", pod, ntp, revision);
//             auto log = s_stack.storage->log_mgr()
//                          .manage(
//                            make_ntp_config(ntp, dir, revision),
//                            raft::group_id(1000 + part),
//                            model::offset_translator_batch_types())
//                          .get();
//             log->start(std::nullopt).get();
//             {
//                 kafka::group_recovery_consumer consumer(
//                   kafka::make_consumer_offsets_serializer(), as);
//                 auto rdr = make_reader(log).get();
//                 auto g_state
//                   = rdr.consume(std::move(consumer),
//                   model::no_timeout).get();
//                 fmt::print(
//                   ">>> pod: {} partition: {} groups: {}\n",
//                   pod,
//                   part,
//                   fmt::join(std::views::keys(g_state.groups), ", "));
//                 groups[pod][part] = std::move(g_state);
//             }

//             s_stack.storage->log_mgr().shutdown(ntp).get();
//             //}
//         }
//         s_stack.stop().get();
//     }

//     chunked_hash_map<
//       kafka::group_id,
//       chunked_hash_map<int, std::unique_ptr<kafka::group_stm>>>
//       groups_stms;

//     for (auto& [pod, partition_groups] : groups) {
//         for (auto& [partition, group_state] : partition_groups) {
//             for (auto& [group_id, group_stm] : group_state.groups) {
//                 fmt::print(
//                   ">>> pod: {} group_id: {} partition: {}\n",
//                   pod,
//                   group_id,
//                   partition);
//                 // fmt::print("group_stm: {}\n", group_stm);
//                 groups_stms[group_id][pod] =
//                 std::make_unique<kafka::group_stm>(
//                   std::move(group_stm));
//             }
//         }
//     }
//     for (auto& [group_id, pod_groups] : groups_stms) {
//         chunked_hash_map<
//           model::topic_partition,
//           chunked_hash_map<int, kafka::group_stm::logged_metadata>>
//           replica_offsets;
//         fmt::print(
//           ">>> group_id: {} pods: {}\n",
//           group_id,
//           fmt::join(std::views::keys(pod_groups), ", "));

//         for (auto& [pod, group_stm] : pod_groups) {
//             for (auto& [partition, omd] : group_stm->offsets()) {
//                 replica_offsets[partition][pod] = omd;
//             }
//         }
//         for (auto& [partition, pod_offsets] : replica_offsets) {
//             absl::node_hash_set<model::offset> offsets;
//             for (auto& [pod, omd] : pod_offsets) {
//                 fmt::print(
//                   ">>> group_id: {} partition: {} pod: {} offset: {} data: "
//                   "{}\n",
//                   group_id,
//                   partition,
//                   pod,
//                   omd.log_offset,
//                   omd.metadata);
//                 offsets.insert(omd.metadata.offset);
//             }
//             if (offsets.size() > 1) {
//                 fmt::print(
//                   ">>> INCONSISTENCY group_id: {} partition: {} offsets:
//                   {}\n", group_id, partition, fmt::join(offsets, ", "));
//             }
//         }
//     }

//     ASSERT_FALSE(true);
// }

struct compaction_test_state {
  chunked_vector<std::pair<ss::sstring, std::optional<size_t>>>
  advance_state() {
      chunked_vector<std::pair<ss::sstring, std::optional<size_t>>> ret;
      for (size_t i = 0; i < random_generators::get_int<size_t>(1, per_step);
           ++i) {
          auto key = fmt::format(
            "key-{}", random_generators::get_int<size_t>(0, keys));
          if (random_generators::get_int(0, 10000) < 10) {
              key = fmt::format(
                "special-{}", random_generators::get_int(0, 3));
          }
          if (random_generators::get_int(0, 10000) < 10) {
              kv_map.erase(key);
              ret.emplace_back(key, std::nullopt);
          } else {
              auto [it, success] = kv_map.try_emplace(key, 0);
              it->second += 1;
              ret.emplace_back(key, it->second);
          }
      }
      return ret;
  }

  model::record_batch_reader make_batch() {
      if (random_generators::get_int(0, 1000) < 2) {
          storage::record_batch_builder builder(
            random_generators::random_choice(
              {model::record_batch_type::raft_configuration,
               model::record_batch_type::archival_metadata}),
            model::offset(0));
          term++;

          builder.add_raw_kv(
            serde::to_iobuf(ss::sstring("test-cfg")), std::nullopt);
          ss::circular_buffer<model::record_batch> batches;
          batches.push_back(std::move(builder).build());

          batches.front().set_term(term);
          return model::make_memory_record_batch_reader(std::move(batches));
      }
      auto records = advance_state();

      storage::record_batch_builder builder(
        model::record_batch_type::raft_data, model::offset(0));
      for (auto& r : records) {
          if (r.second.has_value()) {
              builder.add_raw_kv(
                serde::to_iobuf(r.first), serde::to_iobuf(r.second.value()));
          } else {
              // tombstone
              builder.add_raw_kv(serde::to_iobuf(r.first), std::nullopt);
          }
      }

      ss::circular_buffer<model::record_batch> batches;
      batches.push_back(std::move(builder).build());

      batches.front().set_term(term);
      return model::make_memory_record_batch_reader(std::move(batches));
  }

  static ss::future<compaction_test_state>
  recover_state(ss::shared_ptr<storage::log> log);

  static constexpr size_t keys = 1000;
  static constexpr size_t per_step = 200;
  chunked_hash_map<ss::sstring, size_t> kv_map;
  model::term_id term{0};
};

struct state_recovery_consumer {
  ss::future<ss::stop_iteration>
  operator()(const model::record_batch& batch) {
      if (
        batch.header().type == model::record_batch_type::raft_configuration) {
          co_return ss::stop_iteration::no;
      }
      batch.for_each_record(
        [&](model::record r) { handle_record(std::move(r)); });
      co_return ss::stop_iteration::no;
  }
  void handle_record(model::record r) {
      auto k = serde::from_iobuf<ss::sstring>(r.release_key());

      if (r.has_value()) {
          auto v = serde::from_iobuf<size_t>(r.release_value());
          state.kv_map[k] = v;
      } else {
          state.kv_map.erase(k);
      }
  }

  compaction_test_state end_of_stream() { return std::move(state); }

  compaction_test_state state;
};

ss::future<compaction_test_state>
compaction_test_state::recover_state(ss::shared_ptr<storage::log> log) {
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
  auto state = co_await reader.for_each_ref(
    state_recovery_consumer{}, model::no_timeout);
  co_return state;
};
TEST(compacted_reads, state_test) {
  config::shard_local_cfg().log_compaction_use_sliding_window.set_value(true);
  config::shard_local_cfg().storage_compaction_index_memory.set_value(64_KiB);
  config::shard_local_cfg().storage_compaction_key_map_memory.set_value(
    16_KiB);
  config::shard_local_cfg()
    .storage_compaction_key_map_memory_limit_percent.set_value(1);
  ss::abort_source as;
  chunked_hash_map<
    int,
    chunked_hash_map<int, kafka::group_recovery_consumer_state>>
    groups;

  storage_stack s_stack;
  auto dir = fmt::format(
    "/home/mmaslanka/dev/support/repro-test",
    random_generators::gen_alphanum_string(8));
  s_stack.start(ss::sstring(dir)).get();
  compaction_test_state state;
  auto part = 12;
  model::ntp ntp(
    model::kafka_namespace,
    model::kafka_consumer_offsets_topic,
    model::partition_id(0));
  model::offset max_collectible{0};

  auto log = s_stack.storage->log_mgr()
               .manage(
                 make_ntp_config(ntp, dir, model::revision_id(0)),
                 raft::group_id(1000 + part),
                 model::offset_translator_batch_types())
               .get();
  log->start(std::nullopt).get();
  state = compaction_test_state::recover_state(log).get();
  state.term = log->offsets().committed_offset_term;

  bool stop = false;
  bool stop_hs = false;
  size_t last_offset = log->offsets().committed_offset;
  size_t cnt = 0;
  auto produce_fiber = ss::do_until(
    [&] { return stop; },
    [&] {
        auto appender = log->make_appender(storage::log_append_config{
          .should_fsync = storage::log_append_config::fsync::no,
          .io_priority = ss::default_priority_class(),
          .timeout = model::no_timeout});
        return state.make_batch()
          .for_each_ref(std::move(appender), model::no_timeout)
          .then([&](auto res) {
              cnt += res.last_offset - last_offset;
              last_offset = res.last_offset;
              if (cnt > 10000000) {
                  stop = true;
              }
              if (random_generators::get_int(0, 100) < 2) {
                  max_collectible = res.last_offset;
              }
          })
          .handle_exception([&](const std::exception_ptr& e) {
              fmt::print(">>> produce exception: {}\n", e);
          });
    });

  auto gc_fiber = ss::do_until(
    [&] { return stop_hs; },
    [&] {
        return log->housekeeping(storage::housekeeping_config(
          model::timestamp::max(),
          std::nullopt,
          max_collectible,
          ss::default_priority_class(),
          as,
          std::nullopt));
    });

  fmt::print(">>> PRODUCING...\n");
  produce_fiber.get();
  log->flush().get();

  fmt::print(">>> WAITING FOR COMPACTION\n");
  ss::sleep(20s).get();
  as.request_abort();
  stop_hs = true;
  fmt::print(">>> Recovering...\n");
  auto recovered = compaction_test_state::recover_state(log).get();
  gc_fiber.get();

  for (auto& [k, v] : state.kv_map) {
      fmt::print("key: {} value: {}\n", k, v);
      ASSERT_EQ(recovered.kv_map[k], v);
  }
}
