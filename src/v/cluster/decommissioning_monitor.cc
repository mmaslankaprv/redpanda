#include "cluster/decommissioning_monitor.h"

#include "cluster/logger.h"
#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>

#include <absl/container/flat_hash_set.h>

#include <algorithm>
#include <exception>
#include <iterator>

namespace cluster {

decommissioning_monitor::decommissioning_monitor(
  ss::sharded<cluster::topics_frontend>& fr,
  ss::sharded<topic_table>& tt,
  ss::sharded<partition_allocator>& pal,
  consensus_ptr raft0,
  ss::sharded<ss::abort_source>& as)
  : _topics_frontend(fr)
  , _topics(tt)
  , _allocator(pal)
  , _raft0(raft0)
  , _as(as)
  , _retry_timeout(config::shard_local_cfg().decomission_monitor_retry_ms()) {
    _retry_timer.set_callback([this] {
        // just signal new changes
        _new_changes.signal();
    });
}

ss::future<> decommissioning_monitor::stop() {
    _retry_timer.cancel();
    _new_changes.signal();
    return _bg.close();
}

ss::future<> decommissioning_monitor::start() {
    start_decommission_loop();
    return ss::now();
}

void decommissioning_monitor::start_decommission_loop() {
    return (void)ss::with_gate(_bg, [this] {
        return ss::do_until(
          [this] { return _as.local().abort_requested(); },
          [this] {
              return _new_changes.wait()
                .then([this] {
                    return ss::parallel_for_each(
                      _reallocations, [this](reallocation_meta& meta) {
                          return reallocate_replica_set(meta);
                      });
                })
                .then([this] {
                    // remove finished meta
                    auto it = std::remove_if(
                      std::begin(_reallocations),
                      std::end(_reallocations),
                      [](reallocation_meta& meta) { return meta.is_finished; });

                    _reallocations.erase(it, std::end(_reallocations));
                })
                .then([this] {
                    return _allocator.invoke_on(
                      partition_allocator::shard,
                      [](partition_allocator& allocator) {
                          std::vector<model::node_id> ids;
                          for (const auto& [id, node] :
                               allocator.allocation_nodes()) {
                              if (node->is_decommissioned() && node->empty()) {
                                  ids.push_back(id);
                              }
                          }
                          return ids;
                      });
                })
                .then([this](std::vector<model::node_id> to_remove) {
                    // no nodes to reamove from cluster
                    if (to_remove.empty() || !_raft0->is_leader()) {
                        return ss::now();
                    }
                    vlog(
                      clusterlog.info,
                      "removing decommissioned nodes: {}",
                      to_remove);
                    // we are not interested in the result as we check
                    // configuration in next step
                    return _raft0->remove_members(to_remove).discard_result();
                })
                .then([this] {
                    auto cfg = _raft0->config();
                    if (cfg.type() != raft::configuration_type::simple) {
                        return;
                    }
                    // check results
                    auto it = std::remove_if(
                      std::begin(_decommissioned),
                      std::end(_decommissioned),
                      [&cfg](model::node_id id) {
                          return !cfg.contains_broker(id);
                      });

                    _decommissioned.erase(it, std::end(_decommissioned));
                })
                .then([this] {
                    if (_reallocations.empty() && _decommissioned.empty()) {
                        return;
                    }
                    if (!_retry_timer.armed()) {
                        // arm retry timer
                        _retry_timer.arm(_retry_timeout);
                    }
                })
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      clusterlog.trace,
                      "error handling nodes decomissioning - {}",
                      e);
                });
          });
    });
}

bool is_replica_set_up_to_date(
  const cluster::topic_configuration_assignment& cfg,
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& requested_replica_set) {
    auto it = std::find_if(
      cfg.assignments.begin(),
      cfg.assignments.end(),
      [pid = ntp.tp.partition](const partition_assignment& pas) {
          return pid == pas.id;
      });
    vassert(
      it != cfg.assignments.end(),
      "Reallocated partition {} have to exists ",
      ntp);

    return it->replicas == requested_replica_set;
}

ss::future<>
decommissioning_monitor::reallocate_replica_set(reallocation_meta& meta) {
    // ask partition allocator for reallocation of
    // replicas
    vlog(clusterlog.debug, "reallocating partition {}", meta.ntp);
    auto f = ss::now();
    if (!meta.new_assignment) {
        f = _allocator
              .invoke_on(
                partition_allocator::shard,
                [current = meta.current_assignment](
                  partition_allocator& allocator) mutable {
                    return allocator.reallocate_decommissioned_replicas(
                      current);
                })
              .then([&meta](
                      std::optional<partition_allocator::allocation_units> u) {
                  meta.new_assignment = std::move(u);
              });
    }

    return f.then([this, &meta] {
        if (!meta.new_assignment) {
            return ss::now();
        }
        vlog(
          clusterlog.debug,
          "moving replicas of {} to {}",
          meta.ntp,
          meta.new_assignment->get_assignments().begin()->replicas);
        return _topics_frontend.local()
          .move_partition_replicas(
            meta.ntp,
            meta.new_assignment->get_assignments().begin()->replicas,
            model::no_timeout)
          .then([&meta, this](std::error_code ec) {
              vlog(
                clusterlog.debug, "moving replicas result: {}", ec.message());
              auto it = _topics.local().topics_map().find(
                model::topic_namespace_view(meta.ntp));
              // topic was deleted, we are done with assignment
              if (it == _topics.local().topics_map().end()) {
                  meta.is_finished = true;
                  return;
              }
              meta.is_finished = is_replica_set_up_to_date(
                it->second,
                meta.ntp,
                meta.new_assignment->get_assignments().begin()->replicas);
          });
    });
}

bool is_in_replica_set(
  const std::vector<model::broker_shard>& replicas,
  const absl::flat_hash_set<model::node_id>& ids) {
    auto it = std::find_if(
      std::cbegin(replicas),
      std::cend(replicas),
      [&ids](const model::broker_shard& bs) {
          return ids.contains(bs.node_id);
      });

    return it != std::cend(replicas);
}

absl::flat_hash_set<model::node_id> ids_set(std::vector<model::node_id> ids) {
    absl::flat_hash_set<model::node_id> res;
    res.reserve(ids.size());
    for (auto& id : ids) {
        res.emplace(id);
    }

    return res;
}
bool decommissioning_monitor::is_decommissioned(model::node_id id) const {
    return std::find(
             std::cbegin(_decommissioned), std::cend(_decommissioned), id)
           != _decommissioned.cend();
}

void decommissioning_monitor::decommission(std::vector<model::node_id> ids) {
    absl::flat_hash_set<model::node_id> set_ids;
    for (auto id : ids) {
        if (!is_decommissioned(id)) {
            set_ids.emplace(id);
            _decommissioned.push_back(id);
        }
    }

    for (const auto& [tp_ns, cfg] : _topics.local().topics_map()) {
        for (auto& pas : cfg.assignments) {
            if (is_in_replica_set(pas.replicas, set_ids)) {
                _reallocations.emplace_back(
                  model::ntp(tp_ns.ns, tp_ns.tp, pas.id), pas);
            }
        }
    }
    _new_changes.signal();
}

} // namespace cluster
