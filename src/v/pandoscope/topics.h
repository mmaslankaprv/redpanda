#pragma once
#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"

namespace pandoscope {

struct topics_state {
    static constexpr auto commands = cluster::make_commands_list<
      cluster::create_topic_cmd,
      cluster::delete_topic_cmd,
      cluster::move_partition_replicas_cmd,
      cluster::finish_moving_partition_replicas_cmd,
      cluster::update_topic_properties_cmd,
      cluster::create_partition_cmd,
      cluster::create_non_replicable_topic_cmd>();

    ss::future<> apply(model::record_batch);

    bool is_applicable(const model::record_batch& b) {
        return b.header().type
               == model::record_batch_type::topic_management_cmd;
    }

    ss::future<> apply_all(model::record_batch_reader);

    static auto deserialize_command(model::record_batch b) {
        return cluster::deserialize(std::move(b), commands);
    }

    cluster::topic_table topics;
};

}; // namespace pandoscope
