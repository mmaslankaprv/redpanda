#pragma once
#include "cluster/types.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
#include "raft/heartbeat_manager.h"
#include "raft/rpc_client_protocol.h"
#include "raft/types.h"

#include <seastar/core/metrics_registration.hh>

#include <absl/container/flat_hash_map.h>

namespace raft {

/*
 * Owns and manages all raft groups.
 */
class group_manager {
public:
    using leader_cb_t = ss::noncopyable_function<void(
      raft::group_id, model::term_id, std::optional<model::node_id>)>;

    group_manager(
      model::node_id self,
      model::timeout_clock::duration disk_timeout,
      std::chrono::milliseconds heartbeat_interval,
      ss::sharded<rpc::connection_cache>& clients);

    ss::future<> start();
    ss::future<> stop();

    ss::future<ss::lw_shared_ptr<raft::consensus>> start_group(
      raft::group_id id,
      std::vector<model::broker> nodes,
      storage::log log,
      std::optional<raft::consensus::append_entries_cb_t> append_entries_cb);

    ss::future<> stop_group(ss::lw_shared_ptr<raft::consensus>);

    cluster::notification_id_type
    register_leadership_notification(leader_cb_t cb) {
        auto id = _notification_id++;
        _notifications.push_back(std::make_pair(id, std::move(cb)));
        return id;
    }

    void unregister_leadership_notification(cluster::notification_id_type id) {
        auto it = std::find_if(
          _notifications.begin(),
          _notifications.end(),
          [id](const std::pair<cluster::notification_id_type, leader_cb_t>& n) {
              return n.first == id;
          });
        if (it != _notifications.end()) {
            _notifications.erase(it);
        }
    }

private:
    void trigger_leadership_notification(raft::leadership_status);
    void setup_metrics();

    model::node_id _self;
    model::timeout_clock::duration _disk_timeout;
    raft::consensus_client_protocol _client;
    raft::heartbeat_manager _heartbeats;
    ss::gate _gate;
    std::vector<ss::lw_shared_ptr<raft::consensus>> _groups;
    cluster::notification_id_type _notification_id{0};
    std::vector<std::pair<cluster::notification_id_type, leader_cb_t>>
      _notifications;
    ss::metrics::metric_groups _metrics;
};

} // namespace raft