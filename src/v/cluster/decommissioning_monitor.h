#pragma once

#include "cluster/partition_allocator.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "model/metadata.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
namespace cluster {

class decommissioning_monitor {
public:
    struct reallocation_meta {
        reallocation_meta(model::ntp n, cluster::partition_assignment curr)
          : ntp(std::move(n))
          , current_assignment(std::move(curr)) {}

        model::ntp ntp;
        cluster::partition_assignment current_assignment;
        std::optional<partition_allocator::allocation_units> new_assignment;
        bool is_finished = false;
    };

    decommissioning_monitor(
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<partition_allocator>&,
      consensus_ptr,
      ss::sharded<ss::abort_source>&);

    ss::future<> stop();
    ss::future<> start();

    void decommission(std::vector<model::node_id>);
    void recommission(std::vector<model::node_id>);

    void signal_allocator_updated() { _new_changes.signal(); }

private:
    void start_decommission_loop();
    ss::future<> do_decommission(model::node_id);
    ss::future<> reallocate_replica_set(reallocation_meta&);
    bool is_decommissioned(model::node_id) const;

    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<topic_table>& _topics;
    ss::sharded<partition_allocator>& _allocator;
    consensus_ptr _raft0;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _bg;
    std::vector<model::node_id> _decommissioned;
    std::vector<reallocation_meta> _reallocations;
    ss::condition_variable _new_changes;
    std::chrono::milliseconds _retry_timeout;
    ss::timer<> _retry_timer;
};

} // namespace cluster
