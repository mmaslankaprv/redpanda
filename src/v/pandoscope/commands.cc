
#include "model/fundamental.h"
#include "model/namespace.h"
#include "pandoscope/executor.h"
#include "pandoscope/logger.h"
#include "pandoscope/offset_translator.h"
#include "raft/types.h"
#include "storage/fwd.h"

#include <seastar/core/coroutine.hh>

namespace pandoscope {

bytes serialize_group_key(raft::group_id group, raft::metadata_key key_type) {
    iobuf buf;
    reflection::serialize(buf, key_type, group);
    return iobuf_to_bytes(buf);
}

pandoscope::executor::command reset_raft_kvstore_config(model::ntp ntp) {
    struct cmd : executor::command::impl {
        explicit cmd(model::ntp ntp)
          : _ntp(std::move(ntp)) {}

        ss::future<> execute(storage::log log, executor::ctx ctx) final {
            raft::group_id group_id;
            if (_ntp == model::controller_ntp) {
                group_id = raft::group_id{0};
            } else {
                auto assignment = ctx.topics.topics.get_partition_assignment(
                  _ntp);
                if (!assignment) {
                    co_return;
                }
                group_id = assignment->group;
            }
            vlog(logger.info, "removing state of {} raft group", group_id);
            for (auto& kv : ctx.kvs) {
                co_await kv->remove(
                  storage::kvstore::key_space::consensus,
                  serialize_group_key(
                    group_id, raft::metadata_key::config_latest_known_offset));
                co_await kv->remove(
                  storage::kvstore::key_space::consensus,
                  serialize_group_key(
                    group_id, raft::metadata_key::config_map));
                co_await kv->remove(
                  storage::kvstore::key_space::consensus,
                  serialize_group_key(
                    group_id, raft::metadata_key::config_next_cfg_idx));
            }
            co_await ctx.storage.kvs().remove(
              storage::kvstore::key_space::consensus,
              serialize_group_key(
                group_id, raft::metadata_key::config_latest_known_offset));
            co_await ctx.storage.kvs().remove(
              storage::kvstore::key_space::consensus,
              serialize_group_key(group_id, raft::metadata_key::config_map));
            co_await ctx.storage.kvs().remove(
              storage::kvstore::key_space::consensus,
              serialize_group_key(
                group_id, raft::metadata_key::config_next_cfg_idx));
        }

        std::string_view name() const final {
            return "kvstore_raft_cfg_reset";
        };

        model::ntp _ntp;
    };

    return executor::command(ss::make_shared<cmd>(std::move(ntp)));
}

} // namespace pandoscope
