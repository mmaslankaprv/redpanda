#include "cluster/types.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "test_utils/test.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/file.hh>

#include <cstdint>
#include <optional>
#include <string_view>

using namespace std::chrono_literals;

std::string_view dir = "/home/mmaslanka/dev/support/data_override";
storage::kvstore_config kv_cfg() {
    return {10_MiB, config::mock_binding(10ms), ss::sstring(dir), std::nullopt};
}

storage::log_config log_mgr_cfg() { return {ss::sstring(dir), 1_MiB}; }

TEST_CORO(kv_override, override_configuration) {
    try {
        co_await ss::recursive_remove_directory(
          std::filesystem::path(dir) / "redpanda");
        co_await ss::recursive_remove_directory(
          std::filesystem::path(dir) / "kafka");
    } catch (...) {
    }
    ss::sharded<features::feature_table> ft;

    co_await ft.start();
    storage::api storage(&kv_cfg, &log_mgr_cfg, ft);
    co_await storage.start();
    auto& kvstore = storage.kvs();
    static const bytes node_uuid_key = "node_uuid";

    const bytes invariants_key{"configuration_invariants"};

    // OVERRIDES HERE
    model::node_uuid node_uuid(
      uuid_t::from_string("755ea266-044a-4609-974d-3fc55864c44c"));
    model::node_id node_id{0};
    uint16_t cores = 1;

    co_await kvstore.put(
      storage::kvstore::key_space::controller,
      node_uuid_key,
      serde::to_iobuf(node_uuid));

    auto invariants_buffer = reflection::to_iobuf(
      cluster::configuration_invariants(node_id, cores));

    co_await kvstore.put(
      storage::kvstore::key_space::controller,
      invariants_key,
      std::move(invariants_buffer));

    co_await storage.stop();
    co_await ft.stop();
}
