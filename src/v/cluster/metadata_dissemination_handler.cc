// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_dissemination_handler.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "cluster/metadata_cache.h"
#include "cluster/metadata_dissemination_rpc_service.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/metadata_dissemination_utils.h"
#include "cluster/partition_leaders_table.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "rpc/connection_cache.h"

namespace cluster {
metadata_dissemination_handler::metadata_dissemination_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<partition_manager>& pm,
  ss::sharded<topic_table>& topics,
  ss::sharded<shard_table>& st,
  ss::sharded<rpc::connection_cache>& connections)
  : metadata_dissemination_rpc_service(sg, ssg)
  , _leaders(leaders)
  , _partition_manager(pm)
  , _topic_table(topics)
  , _shard_table(st)
  , _connections(connections)
  , _self(config::shard_local_cfg().node_id()) {}

ss::future<update_leadership_reply>
metadata_dissemination_handler::update_leadership(
  update_leadership_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_update_leadership(std::move(req));
      });
}

ss::future<update_leadership_reply>
metadata_dissemination_handler::do_update_leadership(
  update_leadership_request&& req) {
    return _leaders
      .invoke_on_all(
        [req = std::move(req)](partition_leaders_table& pl) mutable {
            for (auto& leader : req.leaders) {
                pl.update_partition_leader(
                  leader.ntp, leader.term, leader.leader_id);
            }
        })
      .then([] { return ss::make_ready_future<update_leadership_reply>(); });
}

static get_leadership_reply
make_get_leadership_reply(const partition_leaders_table& leaders) {
    ntp_leaders ret;
    leaders.for_each_leader([&ret](
                              model::topic_namespace_view tp_ns,
                              model::partition_id pid,
                              std::optional<model::node_id> leader,
                              model::term_id term) mutable {
        ret.emplace_back(ntp_leader{
          .ntp = model::ntp(tp_ns.ns, tp_ns.tp, pid),
          .term = term,
          .leader_id = leader});
    });

    return get_leadership_reply{std::move(ret)};
}

ss::future<get_leadership_reply> metadata_dissemination_handler::get_leadership(
  get_leadership_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return ss::make_ready_future<get_leadership_reply>(
            make_get_leadership_reply(_leaders.local()));
      });
}

ss::future<get_partition_update_state_reply>
metadata_dissemination_handler::get_partition_update_state(
  get_partition_update_state_request&& req, rpc::streaming_context&) {
    return ss::with_scheduling_group(
      get_scheduling_group(), [this, req = std::move(req)]() mutable {
          return do_get_partition_update_state(std::move(req));
      });
}

ss::future<get_partition_update_state_reply> query_local_replica(
  model::ntp ntp,
  ss::sharded<cluster::partition_manager>& sharded_pm,
  cluster::shard_table& shard_table) {
    auto shard = shard_table.shard_for(ntp);
    if (!shard) {
        get_partition_update_state_reply reply;
        reply.result = errc::topic_invalid_partitions;
        return ss::make_ready_future<get_partition_update_state_reply>(reply);
    }
    return sharded_pm.invoke_on(
      *shard, [ntp = std::move(ntp)](partition_manager& pm) {
          auto partition = pm.get(ntp);
          get_partition_update_state_reply reply;
          if (!partition) {
              reply.result = errc::topic_invalid_partitions;
              return reply;
          }

          reply.result = errc::success;
          reply.committed_offset = partition->committed_offset();
          reply.last_config_offset
            = partition->get_latest_configuration_offset();
          reply.revision = partition->get_revision_id();
          return reply;
      });
}

ss::future<get_partition_update_state_reply>
metadata_dissemination_handler::do_get_partition_update_state(
  get_partition_update_state_request&& req) {
    auto assignment = _topic_table.local().get_partition_assignment(req.ntp);
    // if topic doesn't exists i.e. it was completely removed
    if (!assignment) {
        get_partition_update_state_reply reply;
        reply.result = errc::partition_not_exists;
        return ss::make_ready_future<get_partition_update_state_reply>(reply);
    }
    // check if we have should have local replica
    auto it = std::find_if(
      assignment->replicas.cbegin(),
      assignment->replicas.cend(),
      [this](const model::broker_shard& bs) { return bs.node_id == _self; });

    // has local replicas
    if (it != assignment->replicas.cend()) {
        return query_local_replica(
          std::move(req.ntp), _partition_manager, _shard_table.local());
    }

    // use random replica as a target node
    auto replica_idx = random_generators::get_int(
      assignment->replicas.size() - 1);

    auto target = assignment->replicas[replica_idx].node_id;

    // dispatch to one of the current replica
    return request_partition_update_state(
      _connections.local(), target, std::move(req.ntp));
}

} // namespace cluster
