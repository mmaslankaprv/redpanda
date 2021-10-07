// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller_api.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_service.h"
#include "cluster/data_policy_frontend.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/members_backend.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/raft0_utils.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/security_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "security/acl.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

namespace cluster {

controller::controller(
  ss::sharded<rpc::connection_cache>& ccache,
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  ss::sharded<storage::api>& storage,
  ss::sharded<raft::group_manager>& raft_manager,
  ss::sharded<v8_engine::data_policy_table>& data_policy_table)
  : _connections(ccache)
  , _partition_manager(pm)
  , _shard_table(st)
  , _storage(storage)
  , _tp_updates_dispatcher(_partition_allocator, _tp_state)
  , _security_manager(_credentials, _authorizer)
  , _data_policy_manager(data_policy_table)
  , _raft_manager(raft_manager) {}

ss::future<> controller::wire_up() {
    return _as.start()
      .then([this] { return _members_table.start(); })
      .then([this] { return _partition_allocator.start_single(); })
      .then([this] { return _credentials.start(); })
      .then([this] { return _authorizer.start(); })
      .then([this] { return _tp_state.start(); });
}

ss::future<> controller::start() {
    std::vector<model::broker> initial_raft0_brokers;
    if (config::shard_local_cfg().seed_servers().empty()) {
        initial_raft0_brokers.push_back(
          cluster::make_self_broker(config::shard_local_cfg()));
    }
    return create_raft0(
             _partition_manager,
             _shard_table,
             config::shard_local_cfg().data_directory().as_sstring(),
             std::move(initial_raft0_brokers))
      .then([this](consensus_ptr c) { _raft0 = c; })
      .then([this] { return _partition_leaders.start(std::ref(_tp_state)); })
      .then([this] {
          return _members_manager.start_single(
            _raft0,
            std::ref(_members_table),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_storage),
            std::ref(_as));
      })
      .then([this] {
          // validate configuration invariants to exit early
          return _members_manager.local().validate_configuration_invariants();
      })
      .then([this] {
          return _stm.start_single(
            std::ref(clusterlog),
            _raft0.get(),
            raft::persistent_last_applied::yes,
            std::ref(_tp_updates_dispatcher),
            std::ref(_security_manager),
            std::ref(_members_manager),
            std::ref(_data_policy_manager));
      })
      .then([this] {
          return _members_frontend.start(
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_leaders),
            std::ref(_as));
      })
      .then([this] {
          return _security_frontend.start(
            _raft0->self().id(),
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_leaders),
            std::ref(_as),
            std::ref(_authorizer));
      })
      .then([this] {
          return _data_policy_frontend.start(std::ref(_stm), std::ref(_as));
      })
      .then([this] {
          return _tp_frontend.start(
            _raft0->self().id(),
            std::ref(_stm),
            std::ref(_connections),
            std::ref(_partition_allocator),
            std::ref(_partition_leaders),
            std::ref(_tp_state),
            std::ref(_data_policy_frontend),
            std::ref(_as));
      })
      .then([this] {
          return _members_backend.start_single(
            std::ref(_tp_frontend),
            std::ref(_tp_state),
            std::ref(_partition_allocator),
            std::ref(_members_table),
            std::ref(_api),
            std::ref(_members_manager),
            std::ref(_members_frontend),
            _raft0,
            std::ref(_as));
      })
      .then([this] {
          return _backend.start(
            std::ref(_tp_state),
            std::ref(_shard_table),
            std::ref(_partition_manager),
            std::ref(_members_table),
            std::ref(_partition_leaders),
            std::ref(_tp_frontend),
            std::ref(_storage),
            std::ref(_as));
      })
      .then([this] {
          return _authorizer.invoke_on_all([](security::authorizer& auth) {
              for (auto username : config::shard_local_cfg().superusers()) {
                  auth.add_superuser(security::acl_principal(
                    security::principal_type::user, std::move(username)));
              }
          });
      })
      .then([this] {
          return _members_manager.invoke_on(
            members_manager::shard, &members_manager::start);
      })
      .then([this] {
          /**
           * Controller state machine MUST be started after all entities that
           * receives `apply_update` notifications
           */
          return _stm.invoke_on(controller_stm_shard, &controller_stm::start);
      })
      .then([this] {
          return _stm.invoke_on(controller_stm_shard, [](controller_stm& stm) {
              // we do not have to use timeout in here as all the batches to
              // apply have to be accesssible
              return stm.wait(stm.bootstrap_last_applied(), model::no_timeout);
          });
      })
      .then(
        [this] { return _backend.invoke_on_all(&controller_backend::start); })
      .then([this] {
          return _api.start(
            _raft0->self().id(),
            std::ref(_backend),
            std::ref(_tp_state),
            std::ref(_shard_table),
            std::ref(_connections),
            std::ref(_as));
      })
      .then([this] {
          return _members_backend.invoke_on(
            members_manager::shard, &members_backend::start);
      })
      .then([this] {
          if (!config::shard_local_cfg().enable_leader_balancer()) {
              return ss::now();
          }
          _leader_balancer = std::make_unique<leader_balancer>(
            _tp_state.local(),
            _partition_leaders.local(),
            _raft_manager.local().raft_client(),
            std::ref(_shard_table),
            std::ref(_partition_manager),
            std::ref(_raft_manager),
            std::ref(_as),
            config::shard_local_cfg().leader_balancer_idle_timeout(),
            config::shard_local_cfg().leader_balancer_mute_timeout(),
            config::shard_local_cfg().leader_balancer_node_mute_timeout(),
            _raft0);
          return _leader_balancer->start();
      })
      .then([this] {
          return _health_manager.start_single(
            _raft0->self().id(),
            config::shard_local_cfg().internal_topic_replication_factor(),
            config::shard_local_cfg().health_manager_tick_interval(),
            std::ref(_tp_state),
            std::ref(_tp_frontend),
            std::ref(_partition_allocator),
            std::ref(_partition_leaders),
            std::ref(_as));
      })
      .then([this] {
          return _health_manager.invoke_on(
            health_manager::shard, &health_manager::start);
      });
}

ss::future<> controller::shutdown_input() {
    _raft0->shutdown_input();
    return _as.invoke_on_all(&ss::abort_source::request_abort);
}

ss::future<> controller::stop() {
    if (unlikely(!_raft0)) {
        co_return;
    }

    if (!_as.local().abort_requested()) {
        vlog(clusterlog.info, "DBG: shutting down input");
        co_await shutdown_input();
        vlog(clusterlog.info, "DBG: shut down input");
    }

    if (_leader_balancer) {
        vlog(clusterlog.info, "DBG: stopping leader_balancer");
        co_await _leader_balancer->stop();
        vlog(clusterlog.info, "DBG: stopped leader_balancer");
    }

    vlog(clusterlog.info, "DBG: stopping health_manager");
    co_await _health_manager.stop();
    vlog(clusterlog.info, "DBG: stopped health_manager");
    vlog(clusterlog.info, "DBG: stopping members_backend");
    co_await _members_backend.stop();
    vlog(clusterlog.info, "DBG: stopped members_backend");
    vlog(clusterlog.info, "DBG: stopping api");
    co_await _api.stop();
    vlog(clusterlog.info, "DBG: stopped api");
    vlog(clusterlog.info, "DBG: stopping backend");
    co_await _backend.stop();
    vlog(clusterlog.info, "DBG: stopped backend");
    vlog(clusterlog.info, "DBG: stopping tp_frontend");
    co_await _tp_frontend.stop();
    vlog(clusterlog.info, "DBG: stopped tp_frontend");
    vlog(clusterlog.info, "DBG: stopping security_frontend");
    co_await _security_frontend.stop();
    vlog(clusterlog.info, "DBG: stopped security_frontend");
    vlog(clusterlog.info, "DBG: stopping data_policy_frontend");
    co_await _data_policy_frontend.stop();
    vlog(clusterlog.info, "DBG: stopped data_policy_frontend");
    vlog(clusterlog.info, "DBG: stopping members_frontend");
    co_await _members_frontend.stop();
    vlog(clusterlog.info, "DBG: stopped members_frontend");
    vlog(clusterlog.info, "DBG: stopping stm");
    co_await _stm.stop();
    vlog(clusterlog.info, "DBG: stopped stm");
    vlog(clusterlog.info, "DBG: stopping authorizer");
    co_await _authorizer.stop();
    vlog(clusterlog.info, "DBG: stopped authorizer");
    vlog(clusterlog.info, "DBG: stopping credentials");
    co_await _credentials.stop();
    vlog(clusterlog.info, "DBG: stopped credentials");
    vlog(clusterlog.info, "DBG: stopping tp_state");
    co_await _tp_state.stop();
    vlog(clusterlog.info, "DBG: stopped tp_state");
    vlog(clusterlog.info, "DBG: stopping members_manager");
    co_await _members_manager.stop();
    vlog(clusterlog.info, "DBG: stopped members_manager");
    vlog(clusterlog.info, "DBG: stopping partition_allocator");
    co_await _partition_allocator.stop();
    vlog(clusterlog.info, "DBG: stopped partition_allocator");
    vlog(clusterlog.info, "DBG: stopping partition_leaders");
    co_await _partition_leaders.stop();
    vlog(clusterlog.info, "DBG: stopped partition_leaders");
    vlog(clusterlog.info, "DBG: stopping members_table");
    co_await _members_table.stop();
    vlog(clusterlog.info, "DBG: stopped members_table");
    vlog(clusterlog.info, "DBG: stopping as");
    co_await _as.stop();
    vlog(clusterlog.info, "DBG: stopped as");
}

} // namespace cluster
