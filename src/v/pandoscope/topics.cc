#include "pandoscope/topics.h"

#include "cluster/commands.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

namespace pandoscope {

ss::future<> topics_state::apply(model::record_batch b) {
    auto base_offset = b.base_offset();
    co_await deserialize_command(std::move(b))
      .then([this, base_offset](auto cmd) {
          return ss::visit(
            std::move(cmd),
            [this, base_offset](cluster::delete_topic_cmd cmd) {
                return topics.apply(cmd, base_offset);
            },
            [this, base_offset](cluster::create_topic_cmd cmd) {
                return topics.apply(cmd, base_offset);
            },
            [this, base_offset](cluster::move_partition_replicas_cmd cmd) {
                return topics.apply(cmd, base_offset);
            },
            [this,
             base_offset](cluster::finish_moving_partition_replicas_cmd cmd) {
                return topics.apply(cmd, base_offset);
            },
            [this, base_offset](cluster::update_topic_properties_cmd cmd) {
                return topics.apply(cmd, base_offset);
            },
            [this, base_offset](cluster::create_partition_cmd cmd) {
                return topics.apply(cmd, base_offset);
            },
            [this, base_offset](cluster::create_non_replicable_topic_cmd cmd) {
                return topics.apply(cmd, base_offset);
            });
      });
}

ss::future<> topics_state::apply_all(model::record_batch_reader rdr) {
    struct applying_consumer {
        applying_consumer(topics_state& state)
          : _state(state) {}

        ss::future<ss::stop_iteration> operator()(model::record_batch b) {
            if (_state.is_applicable(b)) {
                co_await _state.apply(std::move(b));
            }
            co_return ss::stop_iteration::no;
        }

        void end_of_stream() {}

        topics_state& _state;
    };

    return std::move(rdr).consume(applying_consumer(*this), model::no_timeout);
}

}; // namespace pandoscope
