#include "kafka/server/group_metadata_migration.h"

#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/fwd.h"
#include "cluster/partition.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"
#include "storage/utils/transforming_reader.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

namespace kafka {

group_metadata_migration::group_metadata_migration(
  cluster::controller& controller, ss::sharded<group_router>& router)
  : _controller(controller)
  , _router(router) {}

bool group_metadata_migration::is_required() {
    // if kafka_internals::group topic is present we still need a migration
    auto md = _controller.get_topics_state().local().get_topic_metadata(
      model::kafka_group_nt);
    return md.has_value();
}

void group_metadata_migration::dispatch_ntp_migration(model::ntp ntp) {
    ssx::spawn_with_gate(
      _partitions_gate, [this, ntp = std::move(ntp)]() mutable {
          return do_dispatch_ntp_migration(std::move(ntp));
      });
}

ss::future<model::offset> get_remote_offset(
  ss::sharded<cluster::partition_manager>& pm,
  model::ntp ntp,
  ss::shard_id shard) {
    return pm.invoke_on(
      shard, [ntp = std::move(ntp)](cluster::partition_manager& pm) {
          auto p = pm.get(ntp);
          if (p) {
              return p->dirty_offset();
          }
          return model::offset{};
      });
}

model::record_batch make_checkpoint_batch() {
    storage::record_batch_builder builder(
      model::record_batch_type::checkpoint, model::offset{0});
    builder.add_raw_kv(iobuf{}, iobuf{});
    return std::move(builder).build();
}

namespace {
ss::future<std::optional<model::record_batch>>
transform_batch(model::record_batch batch) {
    if (batch.header().type == model::record_batch_type::raft_configuration) {
        // replace raft configuration with an empty checkpoint batch, the empty
        // batch will simply be ignored but the source and target topic offsets
        // will match
        co_return make_checkpoint_batch();
    }

    if (batch.header().type == model::record_batch_type::raft_data) {
        auto source_serializer = kafka::make_backward_compatible_serializer();
        auto target_serializer = kafka::make_consumer_offsets_serializer();
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset{});

        for (auto& r : batch.copy_records()) {
            auto tp = source_serializer.get_metadata_type(r.share_key());
            switch (tp) {
            case group_metadata_type::group_metadata: {
                auto md = source_serializer.decode_group_metadata(std::move(r));
                auto kv = target_serializer.to_kv(std::move(md));
                builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
                break;
            }
            case group_metadata_type::offset_commit: {
                auto md = source_serializer.decode_offset_metadata(
                  std::move(r));
                auto kv = target_serializer.to_kv(std::move(md));
                builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
                break;
            }
            case noop:
                builder.add_raw_kv(r.release_key(), r.release_value());
                break;
            }
        }

        co_return std::move(builder).build();
    }
    co_return std::move(batch);
}

ss::future<bool> create_consumer_offsets_topic(
  cluster::controller& controller,
  std::vector<cluster::partition_assignment> assignments,
  model::timeout_clock::time_point timeout) {
    cluster::custom_assignable_topic_configuration topic(
      cluster::topic_configuration{
        model::kafka_namespace,
        model::kafka_consumer_offsets_topic,
        (int32_t)assignments.size(),
        (int16_t)assignments.front().replicas.size()});
    topic.cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    for (auto& p_as : assignments) {
        cluster::custom_partition_assignment custom{.id = p_as.id};
        custom.replicas.reserve(p_as.replicas.size());
        for (auto bs : p_as.replicas) {
            custom.replicas.push_back(bs.node_id);
        }
        topic.custom_assignments.push_back(std::move(custom));
    }

    return controller.get_topics_frontend()
      .local()
      .create_topics({std::move(topic)}, timeout)
      .then([](std::vector<cluster::topic_result> res) {
          /*
           * kindly ask client to retry on error
           */
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              vlog(
                klog.warn,
                "can not create __consumer_offsets topic - {}",
                res[0].ec);
              return false;
          }
          return true;
      })
      .then([&controller, timeout](bool success) {
          if (success) {
              return controller.get_api()
                .local()
                .wait_for_topic(model::kafka_consumer_offsets_tn, timeout)
                .then([](std::error_code ec) { return !ec; });
          }
          return ss::make_ready_future<bool>(success);
      })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(klog.warn, "can not create __consumer_offsets topic - {}", e);
          // various errors may returned such as a timeout, or if the
          // controller group doesn't have a leader. client will retry.
          return false;
      });
}
} // namespace
ss::future<>
group_metadata_migration::do_dispatch_ntp_migration(model::ntp ntp) {
    auto source = _controller.get_partition_manager().local().get(ntp);

    model::ntp target_ntp(
      model::kafka_namespace,
      model::kafka_consumer_offsets_topic,
      source->ntp().tp.partition);

    auto target_shard = _controller.get_shard_table().local().shard_for(
      target_ntp);
    if (!target_shard) {
        co_return;
    }

    auto target_dirty_offset = co_await get_remote_offset(
      _controller.get_partition_manager(), target_ntp, *target_shard);

    while (target_dirty_offset < source->dirty_offset()
           || _controller.get_abort_source().local().abort_requested()) {
        vlog(
          klog.info,
          "migrating {}, source partition end offset: {} target partition end "
          "offset: {} ",
          source->ntp(),
          source->dirty_offset(),
          target_dirty_offset);
        try {
            auto is_leader
              = co_await _controller.get_partition_manager().invoke_on(
                *target_shard, [target_ntp](cluster::partition_manager& pm) {
                    return pm.get(target_ntp)->is_leader();
                });

            if (is_leader) {
                auto reader_cfg = storage::log_reader_config(
                  source->start_offset(),
                  model::offset::max(),
                  ss::default_priority_class());

                auto rdr = co_await source->make_reader(reader_cfg);

                auto t_rdr = storage::make_transforming_reader(
                  std::move(rdr), transform_batch);

                if (t_rdr.is_end_of_stream()) {
                    continue;
                }
                auto f_reader = make_foreign_record_batch_reader(
                  std::move(t_rdr));

                auto result
                  = co_await _controller.get_partition_manager().invoke_on(
                    *target_shard,
                    [target_ntp, f_reader = std::move(f_reader)](
                      cluster::partition_manager& pm) mutable {
                        return pm.get(target_ntp)
                          ->replicate(
                            std::move(f_reader),
                            raft::replicate_options(
                              raft::consistency_level::quorum_ack));
                    });
                if (result.has_error()) {
                    vlog(
                      klog.warn,
                      "migration of {} failed with error: {}",
                      target_ntp,
                      result.error().message());
                } else {
                    vlog(
                      klog.info,
                      "successfull migration replication of {} up to an offset "
                      "of {}",
                      target_ntp,
                      result.value().last_offset);
                    co_return;
                }
            }
            co_await ss::sleep_abortable(
              default_timeout, _controller.get_abort_source().local());

            target_dirty_offset = co_await get_remote_offset(
              _controller.get_partition_manager(), target_ntp, *target_shard);
        } catch (...) {
            vlog(
              klog.warn,
              "error while migrating {} to {} - {}",
              source->ntp(),
              target_ntp,
              std::current_exception());
        }
    }
}

ss::future<> group_metadata_migration::apply(cluster::migration::context ctx) {
    auto& topics = _controller.get_topics_state().local();
    auto group_topic_assignment = topics.get_topic_assignments(
      model::kafka_group_nt);
    auto partitions = group_topic_assignment->size();
    // no group topic found, skip migration
    if (!group_topic_assignment) {
        co_return;
    }
    // delete __consumer_offsets topic to prevent us from recovering partial
    // state
    // TODO: check if finished
    while (topics.contains(
      model::kafka_consumer_offsets_tn, model::partition_id{0})) {
        if (_controller.is_raft0_leader()) {
            co_await _controller.get_topics_frontend().local().delete_topics(
              {model::kafka_consumer_offsets_tn}, default_deadline());
        }
    }

    // create consumer offsets topic based on the old configuration
    while (!topics.contains(
      model::kafka_consumer_offsets_tn, model::partition_id{0})) {
        if (_controller.is_raft0_leader()) {
            co_await create_consumer_offsets_topic(
              _controller,
              std::move(*group_topic_assignment),
              default_deadline());
            continue;
        }

        co_await ss::sleep_abortable(default_timeout, ctx.abort_source);
    }
    co_await _router.invoke_on_all(&group_router::disable);
    absl::node_hash_map<ss::shard_id, std::vector<model::ntp>> shard_ntps;
    for (int16_t p_id = 0; p_id < (int16_t)partitions; ++p_id) {
        model::ntp ntp(
          model::kafka_internal_namespace,
          model::kafka_group_topic,
          model::partition_id(p_id));

        auto shard_id = _controller.get_shard_table().local().shard_for(ntp);

        if (shard_id) {
            shard_ntps[*shard_id].push_back(std::move(ntp));
        }
    }

    for (auto& [shard, ntps] : shard_ntps) {
        co_await ss::smp::submit_to(
          shard,
          [this,
           &pm = _controller.get_partition_manager(),
           ntps = std::move(ntps)]() mutable {
              for (auto& ntp : ntps) {
                  dispatch_ntp_migration(std::move(ntp));
              }
          });
    }

    co_await _partitions_gate.close();

    // delete group topic
    while (topics.contains(model::kafka_group_nt, model::partition_id{0})) {
        if (_controller.is_raft0_leader()) {
            co_await _controller.get_topics_frontend().local().delete_topics(
              {model::kafka_group_nt}, default_deadline());
        }
    }
};

cluster::migration make_group_metadata_migration(
  cluster::controller& controller, ss::sharded<group_router>& router) {
    return cluster::migration(
      std::make_unique<group_metadata_migration>(controller, router));
}
} // namespace kafka
