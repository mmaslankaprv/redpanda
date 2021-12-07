#pragma once
#include "pandoscope/executor.h"
#include "pandoscope/logger.h"
#include "raft/types.h"
#include "storage/offset_translator_state.h"

#include <seastar/core/coroutine.hh>
namespace pandoscope {

enum class kvstore_key_type : int8_t {
    offsets_map = 0,
    highest_known_offset = 1,
};

static bytes
serialize_kvstore_key(raft::group_id group, kvstore_key_type key_type) {
    iobuf buf;
    reflection::serialize(buf, key_type, group);
    return iobuf_to_bytes(buf);
}

inline executor::command print_offset_translator_state() {
    struct cmd : executor::command::impl {
        ss::future<> execute(storage::log log, executor::ctx ctx) final {
            auto ntp = log.config().ntp();
            auto p_as = ctx.topics.topics.get_partition_assignment(ntp);
            auto group = p_as->group;
            vlog(logger.info, "offset translator state... {}", group);

            auto data = ctx.storage.kvs().get(
              storage::kvstore::key_space::consensus,
              serialize_kvstore_key(group, kvstore_key_type::offsets_map));
            if (!data) {
                vlog(logger.info, "offset translator state not present");
                co_return;
            }

            auto state = storage::offset_translator_state::from_serialized_map(
              ntp, std::move(*data));

            vlog(logger.info, "{}", state);
            co_return;
        }

        std::string_view name() const final {
            return "offset translator state printer";
        };
    };

    return executor::make_command<cmd>();
}

} // namespace pandoscope
