#include "cluster/types.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/disk_log_impl.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/segment.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/fixture.h"

FIXTURE_TEST(kv_override, storage_test_fixture) {
    auto cfg = default_log_config("/home/mmaslanka/dev/support/sharechat/data");
    cfg.cache = storage::with_cache::no;
    storage::ntp_config::default_overrides overrides;
    ss::abort_source as;
    storage::log_manager mgr = make_log_manager(cfg);
    static const bytes node_uuid_key = "node_uuid";

    const bytes invariants_key{"configuration_invariants"};
    model::node_uuid node_uuid(
      uuid_t::from_string("307a43da-f710-457c-ac55-bdc7f1f67af8"));

    // INFO  2023-06-16 03:56:46,097 [shard 0] cluster - members_manager.cc:329
    // - Node UUID {8e9752a6-9795-4b08-9084-4fbd6f5d34c1} has node ID {3}
    kvstore
      .put(
        storage::kvstore::key_space::controller,
        node_uuid_key,
        serde::to_iobuf(node_uuid))
      .get();

    cluster::configuration_invariants invariants(model::node_id(11), 4);

    auto invariants_buffer = reflection::to_iobuf(std::move(invariants));

    kvstore
      .put(
        storage::kvstore::key_space::controller,
        invariants_key,
        std::move(invariants_buffer))
      .get();

    mgr.stop().get();
}
