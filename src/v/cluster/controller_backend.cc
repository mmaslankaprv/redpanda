// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_backend.h"

#include "cluster/cluster_utils.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/metadata_dissemination_utils.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome.h"
#include "raft/types.h"
#include "random/generators.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <absl/container/flat_hash_set.h>

/// Class that contains the controller state, for now we will have single
/// controller backend

/// on every core, sharded
namespace cluster {

controller_backend::controller_backend(
  ss::sharded<topic_table>& tp_state,
  ss::sharded<shard_table>& st,
  ss::sharded<partition_manager>& pm,
  ss::sharded<members_table>& members,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<rpc::connection_cache>& conn,
  ss::sharded<ss::abort_source>& as)
  : _topics(tp_state)
  , _shard_table(st)
  , _partition_manager(pm)
  , _members_table(members)
  , _partition_leaders_table(leaders)
  , _connections(conn)
  , _self(model::node_id(config::shard_local_cfg().node_id))
  , _data_directory(config::shard_local_cfg().data_directory().as_sstring())
  , _as(as) {}

ss::future<> controller_backend::stop() {
    _housekeeping_timer.cancel();
    return _gate.close();
}

ss::future<> controller_backend::start() {
    return bootstrap_controller_backend().then([this] {
        start_topics_reconciliation_loop();
        _housekeeping_timer.set_callback([this] { housekeeping(); });
        _housekeeping_timer.arm_periodic(std::chrono::seconds(1));
    });
}

ss::future<> controller_backend::bootstrap_controller_backend() {
    if (!_topics.local().has_pending_changes()) {
        vlog(clusterlog.trace, "no pending changes, skipping bootstrap");
        return ss::now();
    }

    return fetch_deltas().then([this] {
        return ss::with_semaphore(
          _topics_sem, 1, [this] { return do_bootstrap(); });
    });
}
bool has_local_replicas(
  model::node_id self, const std::vector<model::broker_shard>& replicas) {
    return std::find_if(
             std::cbegin(replicas),
             std::cend(replicas),
             [self](const model::broker_shard& bs) {
                 return bs.node_id == self && bs.shard == ss::this_shard_id();
             })
           != replicas.cend();
}

ss::future<> controller_backend::do_bootstrap() {
    return ss::parallel_for_each(
      _topic_deltas.begin(),
      _topic_deltas.end(),
      [this](underlying_t::value_type& ntp_deltas) {
          return bootstrap_ntp(ntp_deltas.first, ntp_deltas.second);
      });
}

ss::future<bool>
controller_backend::is_applicable(model::ntp ntp, const topic_table::delta& d) {
    using op_t = topic_table::delta::op_type;

    model::revision_id rev(d.offset());
    // addition or deletion terminate lookup
    if (d.type == op_t::add || d.type == op_t::del) {
        return ss::make_ready_future<bool>(true);
    }

    // no local replicas just move backward
    if (!has_local_replicas(_self, d.p_as.replicas)) {
        return ss::make_ready_future<bool>(false);
    }

    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));
    // configuration doesn't exists
    vassert(cfg, "Unable to find topic config for {}", ntp);
    auto ntp_cfg = cfg->make_ntp_config(_data_directory, ntp.tp.partition, rev);
    const auto ntp_dir = ntp_cfg.work_directory();

    // if an ntp directory for this revision exists it means that the ntp was
    // created, we do not remove ntp directories right after partition replica
    // removal to determine if creation of replica was successfull or if
    // deletion failed
    return ss::file_exists(ntp_dir);
}

ss::future<>
controller_backend::bootstrap_ntp(model::ntp ntp, deltas_t& deltas) {
    // find last delta that has to be applied, then apply all deltas follwing
    // the one found
    return ss::do_with(
             bool{false},
             deltas.rbegin(),
             [this, &deltas, ntp = std::move(ntp)](
               bool& stop, deltas_t::reverse_iterator& it) {
                 return ss::do_until(
                          [&stop, &it, &deltas] {
                              return stop || it == deltas.rend();
                          },
                          [this, &it, &stop, ntp] {
                              return is_applicable(ntp, *it).then(
                                [&stop, &it](bool applicable) {
                                    stop = applicable;
                                    ++it;
                                });
                          })
                   .then([&it, &deltas] {
                       deltas_t res;
                       res.reserve(std::distance(deltas.rbegin(), it));
                       std::move(
                         it.base(), deltas.end(), std::back_inserter(res));
                       deltas = std::move(res);
                   });
             })
      .then([this, &deltas] { return reconcile_ntp(deltas); });
}

ss::future<> controller_backend::fetch_deltas() {
    return _topics.local()
      .wait_for_changes(_as.local())
      .then([this](deltas_t deltas) {
          return ss::with_semaphore(
            _topics_sem, 1, [this, deltas = std::move(deltas)]() mutable {
                for (auto& d : deltas) {
                    auto ntp = d.ntp;
                    _topic_deltas[ntp].push_back(std::move(d));
                }
            });
      });
}

void controller_backend::start_topics_reconciliation_loop() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] {
              return fetch_deltas()
                .then([this] { return reconcile_topics(); })
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      clusterlog.error,
                      "Error while reconciling topics - {}",
                      e);
                });
          });
    });
}

void controller_backend::housekeeping() {
    if (!_topic_deltas.empty() && _topics_sem.available_units() > 0) {
        (void)ss::with_gate(_gate, [this] { return reconcile_topics(); });
    }
}

ss::future<> controller_backend::reconcile_ntp(deltas_t& deltas) {
    return ss::do_with(
      bool{false},
      deltas.cbegin(),
      [this, &deltas](bool& stop, deltas_t::const_iterator& it) {
          return ss::do_until(
                   [&stop, &it, &deltas] {
                       return stop || it == deltas.cend();
                   },
                   [this, &it, &stop] {
                       return execute_partitition_op(*it).then(
                         [&it, &stop](std::error_code ec) {
                             if (ec) {
                                 stop = true;
                                 return;
                             }
                             it++;
                         });
                   })
            .then([&deltas, &it] {
                // remove finished tasks
                deltas.erase(deltas.cbegin(), it);
            });
      });
}

// caller must hold _topics_sem lock
ss::future<> controller_backend::reconcile_topics() {
    return ss::with_semaphore(_topics_sem, 1, [this] {
        if (_topic_deltas.empty()) {
            return ss::now();
        }
        // reconcile NTPs in parallel
        return ss::parallel_for_each(
                 _topic_deltas.begin(),
                 _topic_deltas.end(),
                 [this](underlying_t::value_type& ntp_deltas) {
                     return reconcile_ntp(ntp_deltas.second);
                 })
          .then([this] {
              // cleanup empty NTP keys
              for (auto it = _topic_deltas.cbegin();
                   it != _topic_deltas.cend();) {
                  if (it->second.empty()) {
                      _topic_deltas.erase(it++);
                  } else {
                      ++it;
                  }
              }
          });
    });
}

std::vector<model::broker> create_brokers_set(
  const std::vector<model::broker_shard>& replicas,
  cluster::members_table& members) {
    std::vector<model::broker> brokers;
    brokers.reserve(replicas.size());
    std::transform(
      std::cbegin(replicas),
      std::cend(replicas),
      std::back_inserter(brokers),
      [&members](const model::broker_shard& bs) {
          auto br = members.get_broker(bs.node_id);
          if (!br) {
              throw std::logic_error(
                fmt::format("Replica node {} is not available", bs.node_id));
          }
          return *br->get();
      });
    return brokers;
}

ss::future<std::error_code>
controller_backend::execute_partitition_op(const topic_table::delta& delta) {
    /**
     * Revision is derived from delta offset, i.e. offset of a command that
     * the delta is derived from. The offset is always monotonically
     * increasing together with cluster state evelotion hence it is perfect
     * as a source of revision_id
     */
    vlog(clusterlog.trace, "executing ntp opeartion: {}", delta);
    model::revision_id rev(delta.offset());
    // new partitions

    // only create partitions for this backend
    // partitions created on current shard at this node
    switch (delta.type) {
    case topic_table::delta::op_type::add:
        if (!has_local_replicas(_self, delta.p_as.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_partition(
          delta.ntp,
          delta.p_as.group,
          rev,
          create_brokers_set(delta.p_as.replicas, _members_table.local()));
    case topic_table::delta::op_type::del:
        // delete partition together with directory
        return delete_partition(delta.ntp, rev, true);
    case topic_table::delta::op_type::update:
        return process_partition_update(delta.ntp, delta.p_as, rev);
    }
    __builtin_unreachable();
}

/**
 * This method uses out of band (not being a part of raft protocol) mechanism to
 * determine if it is safe to remove partition replica. In order to safely do
 * that and not comprise liveness of raft protocol the decision has to be
 * requested by node that is going to remove the partiton. Otherwise it the node
 * is permanently down other partitipants wouldn't have confirmation if the
 * replica was removed successfully. Delete is decided using information comming
 * from one of the nodes holding current replica set for given partion. Request
 * target node is chosen from current replica set available in topics table.
 * If the data in table are out of request receiving node will route request
 * to one of the nodes that are currently in replica set.
 *
 * Why we can not relay just on Raft protocol ?
 *
 * In raft all the requests are initiated by the leader. In oreder to make sure
 * that all of the replicas that are designated to be deleted the leader would
 * have to make sure that exactly ALL nodes (from previous configuration) have
 * reached certain offset. When one of the nodes from the previous configuration
 * would be down leader wouldn't be possible to make progress.
 *
 * Important notice:
 *
 * When configuration revision is advanced comparing to current update revision
 * it means that majority of partition replicas considered configuration change
 * as successfull
 *
 * Following conditions trigger deletion of partion replica:
 *
 * 1) Configuration with current or greater revision was committed (no request
 *    is sent)
 *
 * 2) Partion was completely removed from the topics table (i.e. nothing depend
 *    on partion state, it can be safely removed)
 *
 * 3) Response revision_id is greater than current update revision and remote
 *     replica has seen current configuration already
 *
 * 4) Configuration from current update was already committed by other replicas.
 *    This is perfectly normal situation as the node that is leaving replica set
 *    will not get updates after majority replicas committed configuration
 *    confirming node removal.
 */
ss::future<bool> controller_backend::is_ready_to_be_removed(
  ss::lw_shared_ptr<partition> partition, model::revision_id rev) {
    auto cfg_offset = partition->get_latest_configuration_offset();

    // case 1)
    bool is_simple = partition->group_configuration().type()
                     == raft::configuration_type::simple;

    // already committed doesn't have to check with other brokers
    if (
      cfg_offset <= partition->committed_offset() && is_simple
      && partition->get_revision_id() >= rev) {
        return ss::make_ready_future<bool>(true);
    }
    // case 2)
    auto pas = _topics.local().get_partition_assignment(partition->ntp());
    if (!pas) {
        // already removed from state, we can remove this partion
        return ss::make_ready_future<bool>(true);
    }

    // use random replica
    auto idx = random_generators::get_int(pas->replicas.size() - 1);
    auto target = pas->replicas[idx].node_id;
    if (target == _self) {
        return ss::make_ready_future<bool>(false);
    }
    return request_partition_update_state(
             _connections.local(), target, partition->ntp())
      .then([rev, cfg_offset](get_partition_update_state_reply reply) {
          if (reply.result == errc::success) {
              auto cfg_committed = reply.committed_offset >= model::offset(0)
                                   && reply.last_config_offset
                                        <= reply.committed_offset;

              // case 3)
              if (
                reply.revision > rev && reply.committed_offset >= cfg_offset) {
                  return true;
              }
              // case 4)

              return cfg_committed && reply.revision == rev;
          }
          return false;
      });
}

bool is_configuration_up_to_date(
  const ss::lw_shared_ptr<partition>& partition,
  model::revision_id update_revision,
  const std::vector<model::broker_shard>& bs) {
    auto group_cfg = partition->group_configuration();
    auto p_rev = partition->get_revision_id();
    auto configuration_committed = partition->get_latest_configuration_offset()
                                   <= partition->committed_offset();

    if (group_cfg.type() == raft::configuration_type::joint) {
        return false;
    }

    if (p_rev == update_revision && !configuration_committed) {
        return false;
    }
    // compare broker ids
    absl::flat_hash_set<model::node_id> all_ids;
    for (auto& id : group_cfg.current_config().voters) {
        all_ids.emplace(id);
    }

    for (auto& id : group_cfg.current_config().learners) {
        all_ids.emplace(id);
    }

    // there is different number of brokers in group configuration
    if (all_ids.size() != bs.size()) {
        return false;
    }

    for (auto& b : bs) {
        all_ids.emplace(b.node_id);
    }
    return all_ids.size() == bs.size();
}

ss::future<std::error_code> controller_backend::process_partition_update(
  model::ntp ntp, const partition_assignment& current, model::revision_id rev) {
    vlog(clusterlog.trace, "processing partiton update, revision: {}", rev);
    // if there is no local replica in replica set but,
    // partition with requested ntp exists on this broker core
    // it has to be removed after new configuration is stable
    auto partition = _partition_manager.local().get(ntp);

    if (partition && partition->get_revision_id() > rev) {
        // current change is obsolete, configuration is already updated with
        // more recent change, do nothing
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    if (!has_local_replicas(_self, current.replicas)) {
        // we do not have local replicas and partition does not
        // exists, it is ok
        if (!partition) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return is_ready_to_be_removed(partition, rev)
          .then([this, ntp, rev, current](bool can_remove) {
              if (can_remove) {
                  vlog(
                    clusterlog.debug,
                    "removing partition: {} replica, revision: {}",
                    ntp,
                    rev);
                  // do not remove folder to track partition movement
                  return delete_partition(ntp, rev, false);
              }
              return update_partition_replica_set(ntp, current.replicas, rev)
                .then([](std::error_code ec) {
                    // always wait for recovery after configuration update
                    return ec ? ec : errc::waiting_for_recovery;
                });
          });
    }

    // partition already exists, update configuration
    if (partition) {
        return update_partition_replica_set(ntp, current.replicas, rev);
    }
    // create partition with empty configuration. Configuration
    // will be populated during node recovery
    vlog(
      clusterlog.debug,
      "creating partition: {} replica, revision: {}",
      ntp,
      rev);
    return create_partition(ntp, current.group, rev, {})
      .then([](std::error_code ec) {
          // always wait for recovery after creating partiton
          return ec ? ec : errc::waiting_for_recovery;
      });
}

ss::future<std::error_code> controller_backend::update_partition_replica_set(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& replicas,
  model::revision_id rev) {
    /**
     * Following scenarios can happen in here:
     * - node is a leader for current partition => just update config
     * - node is not a leader for current partition => check if config is
     *   equal to requested, if not return failure
     */
    auto partition = _partition_manager.local().get(ntp);
    // wait for configuration update, only declare success
    // when configuration was actually updated
    if (is_configuration_up_to_date(partition, rev, replicas)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    // we are the leader, update configuration
    if (partition->is_leader()) {
        auto brokers = create_brokers_set(replicas, _members_table.local());
        vlog(
          clusterlog.debug,
          "updating partition: {} replica set with: {}, revision: {}",
          ntp,
          replicas,
          rev);

        auto f = partition->update_replica_set(std::move(brokers), rev);
        return ss::with_timeout(
                 model::timeout_clock::now() + std::chrono::seconds(5),
                 std::move(f))
          .then_wrapped([](ss::future<std::error_code> f) {
              try {
                  return f.get0();
              } catch (const ss::timed_out_error& e) {
                  return make_error_code(errc::timeout);
              }
          })
          .then([partition, rev, replicas](std::error_code) {
              if (is_configuration_up_to_date(partition, rev, replicas)) {
                  return make_error_code(errc::success);
              }
              return make_error_code(errc::waiting_for_recovery);
          });
    }

    return ss::make_ready_future<std::error_code>(errc::not_leader);
}

ss::future<> controller_backend::add_to_shard_table(
  model::ntp ntp, raft::group_id raft_group, uint32_t shard) {
    // update shard_table: broadcast
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), raft_group, shard](shard_table& s) mutable {
          s.insert(std::move(ntp), shard);
          s.insert(raft_group, shard);
      });
}

ss::future<std::error_code> controller_backend::create_partition(
  model::ntp ntp,
  raft::group_id group_id,
  model::revision_id rev,
  std::vector<model::broker> members) {
    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));

    if (!cfg) {
        // partition was already removed, do nothing
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    auto f = ss::now();
    // handle partially created topic
    auto partition = _partition_manager.local().get(ntp);
    // no partition exists, create one
    if (likely(!partition)) {
        // we use offset as an rev as it is always increasing and it
        // increases while ntp is being created again
        f = _partition_manager.local()
              .manage(
                cfg->make_ntp_config(_data_directory, ntp.tp.partition, rev),
                group_id,
                std::move(members))
              .discard_result();
    } else {
        // old partition still exists, wait for it to be removed
        if (partition->get_revision_id() < rev) {
            return ss::make_ready_future<std::error_code>(
              errc::partition_already_exists);
        }
    }

    return f
      .then([this, ntp = std::move(ntp), group_id]() mutable {
          // we create only partitions that belongs to current shard
          return add_to_shard_table(
            std::move(ntp), group_id, ss::this_shard_id());
      })
      .then([] { return make_error_code(errc::success); });
}

ss::future<std::error_code> controller_backend::delete_partition(
  model::ntp ntp, model::revision_id rev, bool remove_ntp_dir) {
    auto part = _partition_manager.local().get(ntp);
    if (unlikely(part.get() == nullptr)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    // partition was already recreated with greater rev, do nothing
    if (unlikely(part->get_revision_id() > rev)) {
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    auto group_id = part->group();

    return _shard_table
      .invoke_on_all(
        [ntp, group_id](shard_table& st) mutable { st.erase(ntp, group_id); })
      .then([this, ntp] {
          return _partition_leaders_table.invoke_on_all(
            [ntp](partition_leaders_table& leaders) {
                leaders.remove_leader(ntp);
            });
      })
      .then([this, ntp = std::move(ntp), remove_ntp_dir] {
          // remove partition
          return _partition_manager.local().remove(ntp, remove_ntp_dir);
      })
      .then([] { return make_error_code(errc::success); });
}

} // namespace cluster
