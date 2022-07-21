// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/group_configuration.h"

#include "config/config_store.h"
#include "model/adl_serde.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"

#include <absl/container/flat_hash_set.h>
#include <bits/stdint-uintn.h>
#include <boost/range/join.hpp>

#include <algorithm>
#include <iterator>
#include <optional>
#include <utility>
#include <vector>

namespace raft {
/**
 * Strategy representing the old way of changing configuration i.e. Joint
 * consensus based
 */
class configuration_change_strategy_v3
  : public group_configuration::configuration_change_strategy {
public:
    explicit configuration_change_strategy_v3(group_configuration& cfg)
      : _cfg(cfg) {}

    void add(std::vector<model::broker>, model::revision_id) final;
    void remove(const std::vector<model::node_id>&) final;
    void replace(std::vector<broker_revision>, model::revision_id) final;

    void discard_old_config() final;
    void abort_configuration_change(model::revision_id) final;
    void cancel_configuration_change(model::revision_id) final;

private:
    group_configuration& _cfg;
};
bool group_nodes::contains(const vnode& id) const {
    auto v_it = std::find(voters.cbegin(), voters.cend(), id);
    if (v_it != voters.cend()) {
        return true;
    }
    auto l_it = std::find(learners.cbegin(), learners.cend(), id);
    return l_it != learners.cend();
}

std::optional<vnode> group_nodes::find(model::node_id id) const {
    auto v_it = std::find_if(
      voters.cbegin(), voters.cend(), [id](const vnode& rni) {
          return rni.id() == id;
      });

    if (v_it != voters.cend()) {
        return *v_it;
    }
    auto l_it = std::find_if(
      learners.cbegin(), learners.cend(), [id](const vnode& rni) {
          return rni.id() == id;
      });

    return l_it != learners.cend() ? std::make_optional(*l_it) : std::nullopt;
}

group_configuration::group_configuration(
  std::vector<model::broker> brokers, model::revision_id revision)
  : _brokers(std::move(brokers))
  , _revision(revision) {
    _current.voters.resize(brokers.size());
    std::transform(
      _brokers.cbegin(),
      _brokers.cend(),
      std::back_inserter(_current.voters),
      [revision](const model::broker& br) { return vnode(br.id(), revision); });
}

/**
 * Creates joint configuration
 */
group_configuration::group_configuration(
  std::vector<model::broker> brokers,
  group_nodes current,
  model::revision_id revision,
  std::optional<group_nodes> old)
  : _brokers(std::move(brokers))
  , _current(std::move(current))
  , _old(std::move(old))
  , _revision(revision) {}

std::unique_ptr<group_configuration::configuration_change_strategy>
group_configuration::make_change_strategy() {
    return std::make_unique<configuration_change_strategy_v3>(*this);
}

std::optional<model::broker>
group_configuration::find_broker(model::node_id id) const {
    auto it = std::find_if(
      _brokers.cbegin(), _brokers.cend(), [id](const model::broker& broker) {
          return id == broker.id();
      });

    if (it != _brokers.cend()) {
        return *it;
    }
    return std::nullopt;
}

bool group_configuration::has_voters() const {
    return !(_current.voters.empty() || (_old && _old->voters.empty()));
}

bool group_configuration::is_voter(vnode id) const {
    auto it = std::find(_current.voters.cbegin(), _current.voters.cend(), id);

    if (it != _current.voters.cend()) {
        return true;
    }
    if (!_old) {
        return false;
    }
    auto old_it = std::find(_old->voters.cbegin(), _old->voters.cend(), id);

    return old_it != _old->voters.cend();
}

bool group_configuration::is_allowed_to_request_votes(vnode id) const {
    // either current voter
    auto it = std::find(_current.voters.cbegin(), _current.voters.cend(), id);

    if (it != _current.voters.cend()) {
        return true;
    }
    if (!_old) {
        return false;
    }
    // or present in old configuration
    auto old_it = std::find(_old->voters.cbegin(), _old->voters.cend(), id);

    // present in old voters
    if (old_it != _old->voters.cend()) {
        return true;
    }
    // look in learners
    old_it = std::find(_old->learners.cbegin(), _old->learners.cend(), id);

    return old_it != _old->learners.cend();
}

bool group_configuration::contains_broker(model::node_id id) const {
    auto it = std::find_if(
      _brokers.cbegin(), _brokers.cend(), [id](const model::broker& broker) {
          return id == broker.id();
      });

    return it != _brokers.cend();
}

bool group_configuration::contains_address(
  const net::unresolved_address& address) const {
    return std::any_of(
      _brokers.cbegin(),
      _brokers.cend(),
      [&address](const model::broker& broker) {
          return address == broker.rpc_address();
      });
}

configuration_type group_configuration::type() const {
    if (_old) {
        return configuration_type::joint;
    }
    return configuration_type::simple;
};

std::vector<vnode>
unique_ids(const std::vector<vnode>& current, const std::vector<vnode>& old) {
    absl::flat_hash_set<vnode> unique_ids;
    unique_ids.reserve(current.size());

    for (auto& id : current) {
        unique_ids.insert(id);
    }
    for (auto& id : old) {
        unique_ids.insert(id);
    }
    std::vector<vnode> ret;
    ret.reserve(unique_ids.size());
    std::copy(unique_ids.begin(), unique_ids.end(), std::back_inserter(ret));
    return ret;
}

bool group_configuration::contains(vnode id) const {
    return _current.contains(id) || (_old && _old->contains(id));
}

std::vector<vnode> group_configuration::unique_voter_ids() const {
    auto old_voters = _old ? _old->voters : std::vector<vnode>();
    return unique_ids(_current.voters, old_voters);
}
std::vector<vnode> group_configuration::unique_learner_ids() const {
    auto old_learners = _old ? _old->learners : std::vector<vnode>();
    return unique_ids(_current.learners, old_learners);
}

void erase_id(std::vector<vnode>& v, model::node_id id) {
    auto it = std::find_if(
      v.cbegin(), v.cend(), [id](const vnode& rni) { return id == rni.id(); });

    if (it != v.cend()) {
        v.erase(it);
    }
}

void group_configuration::add(
  std::vector<model::broker> brokers, model::revision_id rev) {
    vassert(
      type() == configuration_type::simple,
      "can not add node to configuration when update is in progress - {}",
      *this);

    make_change_strategy()->add(std::move(brokers), rev);
}

void group_configuration::remove(const std::vector<model::node_id>& ids) {
    vassert(
      type() == configuration_type::simple,
      "can not remove node from configuration when update is in progress - {}",
      *this);
    make_change_strategy()->remove(ids);
}

void group_configuration::replace(
  std::vector<broker_revision> brokers, model::revision_id rev) {
    vassert(
      type() == configuration_type::simple,
      "can not replace configuration when update is in progress - {}",
      *this);
    make_change_strategy()->replace(std::move(brokers), rev);
}

void group_configuration::discard_old_config() {
    vassert(
      type() == configuration_type::joint,
      "can only discard old configuration when in joint state - {}",
      *this);
    make_change_strategy()->discard_old_config();
}

void group_configuration::abort_configuration_change(model::revision_id rev) {
    vassert(
      type() != configuration_type::simple,
      "can not abort configuration change if it is of simple type - {}",
      *this);
    make_change_strategy()->abort_configuration_change(rev);
}

void group_configuration::cancel_configuration_change(model::revision_id rev) {
    vassert(
      type() != configuration_type::simple,
      "can not cancel configuration change if it is of simple type - {}",
      *this);
    make_change_strategy()->cancel_configuration_change(rev);
}

void group_configuration::promote_to_voter(vnode id) {
    auto it = std::find(
      _current.learners.cbegin(), _current.learners.cend(), id);
    // do nothing
    if (it == _current.learners.cend()) {
        return;
    }
    // add to voters
    _current.learners.erase(it);
    _current.voters.push_back(id);
}

bool group_configuration::maybe_demote_removed_voters() {
    vassert(
      _old,
      "can not demote removed voters as configuration is of simple type - {}",
      *this);

    // no voters are present, do nothing
    if (_old->voters.empty()) {
        return false;
    }
    // if voter was removed, make it a learner
    auto it = std::stable_partition(
      _old->voters.begin(), _old->voters.end(), [this](const vnode& v) {
          return _current.contains(v);
      });

    // nothing to remove
    if (std::distance(it, _old->voters.end()) == 0) {
        return false;
    }

    std::move(it, _old->voters.end(), std::back_inserter(_old->learners));
    _old->voters.erase(it, _old->voters.end());

    return true;
}

void group_configuration::update(model::broker broker) {
    auto it = std::find_if(
      _brokers.begin(), _brokers.end(), [id = broker.id()](model::broker& b) {
          return id == b.id();
      });

    if (it == _brokers.end()) {
        throw std::invalid_argument(fmt::format(
          "broker {} does not exists in configuration {}", broker.id(), *this));
    }

    *it = std::move(broker);
}

/**
 * Update strategy for v3 configuration
 */

void configuration_change_strategy_v3::add(
  std::vector<model::broker> brokers, model::revision_id rev) {
    _cfg._revision = rev;
    for (auto& b : brokers) {
        auto it = std::find_if(
          _cfg._brokers.cbegin(),
          _cfg._brokers.cend(),
          [id = b.id()](const model::broker& n) { return id == n.id(); });
        if (unlikely(it != _cfg._brokers.cend())) {
            throw std::invalid_argument(fmt::format(
              "broker {} already present in current configuration {}",
              b.id(),
              _cfg));
        }
    }

    _cfg._old = _cfg._current;
    for (auto& b : brokers) {
        _cfg._current.learners.emplace_back(b.id(), rev);
        _cfg._brokers.push_back(std::move(b));
    }
}

void configuration_change_strategy_v3::remove(
  const std::vector<model::node_id>& ids) {
    for (auto& id : ids) {
        auto broker_it = std::find_if(
          _cfg._brokers.cbegin(),
          _cfg._brokers.cend(),
          [id](const model::broker& n) { return id == n.id(); });
        if (unlikely(broker_it == _cfg._brokers.cend())) {
            throw std::invalid_argument(fmt::format(
              "broker {} not found in current configuration {}", id, _cfg));
        }
    }

    auto new_cfg = _cfg._current;
    // we do not yet remove brokers as we have to know each of them until
    // configuration will be advanced to simple mode
    for (auto& id : ids) {
        erase_id(new_cfg.learners, id);
        erase_id(new_cfg.voters, id);
    }

    _cfg._old = std::move(_cfg._current);
    _cfg._current = std::move(new_cfg);
}

void configuration_change_strategy_v3::replace(
  std::vector<broker_revision> brokers, model::revision_id rev) {
    _cfg._revision = rev;

    /**
     * If configurations are identical do nothing. For identical configuration
     * we assume that brokers list hasn't changed (1) and current configuration
     * contains all brokers in either voters of learners (2).
     */
    // check list of brokers (1)

    // check if all brokers are assigned to current configuration (2)
    bool brokers_are_equal
      = brokers.size() == _cfg._brokers.size()
        && std::all_of(
          brokers.begin(), brokers.end(), [this](const broker_revision& b) {
              // we may do linear lookup in _brokers collection as number of
              // brokers is usually very small f.e. 3 or 5
              auto it = std::find_if(
                _cfg._brokers.begin(),
                _cfg._brokers.end(),
                [&b](const model::broker& existing) {
                    return b.broker == existing;
                });

              return _cfg._current.contains(vnode(b.broker.id(), b.rev))
                     && it != _cfg._brokers.end();
          });

    // configurations are identical, do nothing
    if (brokers_are_equal) {
        return;
    }

    _cfg._old = _cfg._current;
    _cfg._current.learners.clear();
    _cfg._current.voters.clear();

    for (auto& br : brokers) {
        // check if broker is already a voter. voter will stay a voter
        auto v_it = std::find_if(
          _cfg._old->voters.cbegin(),
          _cfg._old->voters.cend(),
          [&br](const vnode& rni) {
              return rni.id() == br.broker.id() && rni.revision() == br.rev;
          });

        if (v_it != _cfg._old->voters.cend()) {
            _cfg._current.voters.push_back(*v_it);
            continue;
        }

        // check if broker was a learner. learner will stay a learner
        auto l_it = std::find_if(
          _cfg._old->learners.cbegin(),
          _cfg._old->learners.cend(),
          [&br](const vnode& rni) {
              return rni.id() == br.broker.id() && rni.revision() == br.rev;
          });

        if (l_it != _cfg._old->learners.cend()) {
            _cfg._current.learners.push_back(*l_it);
            continue;
        }

        // new broker, use broker revision
        _cfg._current.learners.emplace_back(br.broker.id(), br.rev);
    }

    // if both current and previous configurations are exactly the same, we do
    // not need to enter joint consensus
    if (
      _cfg._current.voters == _cfg._old->voters
      && _cfg._current.learners == _cfg._old->learners) {
        _cfg._old.reset();
    }

    for (auto& b : brokers) {
        if (!_cfg.contains_broker(b.broker.id())) {
            _cfg._brokers.push_back(std::move(b.broker));
        }
    }
}

void configuration_change_strategy_v3::abort_configuration_change(
  model::revision_id rev) {
    absl::flat_hash_set<model::node_id> physical_node_ids;

    for (auto& id : _cfg._old->learners) {
        physical_node_ids.insert(id.id());
    }

    for (auto& id : _cfg._old->voters) {
        physical_node_ids.insert(id.id());
    }
    std::erase_if(_cfg._brokers, [&physical_node_ids](model::broker& b) {
        return !physical_node_ids.contains(b.id());
    });
    _cfg._current = *_cfg._old;
    _cfg._old.reset();

    // make sure that all nodes are voters
    for (auto id : _cfg._current.learners) {
        _cfg.promote_to_voter(id);
    }
    _cfg._revision = rev;
}

void configuration_change_strategy_v3::cancel_configuration_change(
  model::revision_id rev) {
    auto tmp = _cfg._current;
    _cfg._current = *_cfg._old;
    _cfg._old = std::move(tmp);
    _cfg._revision = rev;
}

void configuration_change_strategy_v3::discard_old_config() {
    absl::flat_hash_set<model::node_id> physical_node_ids;

    for (auto& id : _cfg._current.learners) {
        physical_node_ids.insert(id.id());
    }

    for (auto& id : _cfg._current.voters) {
        physical_node_ids.insert(id.id());
    }
    // remove unused brokers from brokers set
    auto it = std::stable_partition(
      _cfg._brokers.begin(),
      _cfg._brokers.end(),
      [physical_node_ids](const model::broker& b) {
          return physical_node_ids.contains(b.id());
      });
    // we are only interested in current brokers
    _cfg._brokers.erase(it, _cfg._brokers.end());
    _cfg._old.reset();
}

std::vector<vnode> with_revisions_assigned(
  const std::vector<vnode>& vnodes, model::revision_id new_revision) {
    std::vector<vnode> with_rev;
    with_rev.reserve(vnodes.size());

    std::transform(
      vnodes.cbegin(),
      vnodes.cend(),
      std::back_inserter(with_rev),
      [new_revision](const vnode& n) {
          vassert(
            n.revision() == no_revision,
            "changing revision of nodes with current revision set should never "
            "happen, current revision: {}",
            n.revision());
          return vnode(n.id(), new_revision);
      });

    return with_rev;
}

bool have_no_revision(const std::vector<vnode>& vnodes) {
    return !vnodes.empty() && vnodes.begin()->revision() == no_revision;
}

void group_configuration::maybe_set_initial_revision(
  model::revision_id new_rev) {
    group_nodes new_current;
    // if configuration have no revision assigned, fix it
    if (
      have_no_revision(_current.learners)
      || have_no_revision(_current.voters)) {
        // current configuration
        _current.voters = with_revisions_assigned(_current.voters, new_rev);
        _current.learners = with_revisions_assigned(_current.learners, new_rev);

        // old configuration
        if (_old) {
            _old->voters = with_revisions_assigned(_old->voters, new_rev);
            _old->learners = with_revisions_assigned(_old->learners, new_rev);
        }
    }
}

std::ostream& operator<<(std::ostream& o, const group_configuration& c) {
    fmt::print(
      o,
      "{{current: {}, old:{}, revision: {}, brokers: {}}}",
      c._current,
      c._old,
      c._revision,
      c._brokers);
    return o;
}

std::ostream& operator<<(std::ostream& o, const group_nodes& n) {
    fmt::print(o, "{{voters: {}, learners: {}}}", n.voters, n.learners);
    return o;
}

std::ostream& operator<<(std::ostream& o, const offset_configuration& c) {
    fmt::print(o, "{{offset: {}, group_configuration: {}}}", c.offset, c.cfg);
    return o;
}

std::ostream& operator<<(std::ostream& o, configuration_type t) {
    switch (t) {
    case configuration_type::simple:
        return o << "simple";
    case configuration_type::joint:
        return o << "joint";
    }
    __builtin_unreachable();
}
} // namespace raft

namespace reflection {

void adl<raft::group_configuration>::to(
  iobuf& buf, raft::group_configuration cfg) {
    serialize(
      buf,
      cfg.version(),
      cfg.brokers(),
      cfg.current_config(),
      cfg.old_config(),
      cfg.revision_id());
}

std::vector<raft::vnode> make_vnodes(const std::vector<model::node_id> ids) {
    std::vector<raft::vnode> ret;
    ret.reserve(ids.size());
    std::transform(
      ids.begin(), ids.end(), std::back_inserter(ret), [](model::node_id id) {
          return raft::vnode(id, raft::no_revision);
      });
    return ret;
}

struct group_nodes_v0 {
    std::vector<model::node_id> voters;
    std::vector<model::node_id> learners;

    raft::group_nodes to_v2() {
        raft::group_nodes ret;
        ret.voters = make_vnodes(voters);
        ret.learners = make_vnodes(learners);
        return ret;
    }
};

raft::group_configuration
adl<raft::group_configuration>::from(iobuf_parser& p) {
    auto version = adl<raft::group_configuration::version_t>{}.from(p);
    // currently we support only versions up to 1
    vassert(
      version <= raft::group_configuration::current_version,
      "Version {} is not supported. We only support versions up to {}",
      version,
      raft::group_configuration::current_version);

    /**
     * we use versions field to maintain backward compatibility
     *
     * version 0 - base
     * version 1 - introduced revision id
     * version 2 - introduced raft::vnode
     * version 3 - model::broker with multiple endpoints
     * version 4 - persist configuration update request
     */

    std::vector<model::broker> brokers;

    if (likely(version >= raft::group_configuration::version_t(3))) {
        brokers = adl<std::vector<model::broker>>{}.from(p);
    } else {
        auto brokers_v0 = adl<std::vector<model::internal::broker_v0>>{}.from(
          p);
        std::transform(
          brokers_v0.begin(),
          brokers_v0.end(),
          std::back_inserter(brokers),
          [](const model::internal::broker_v0& broker) {
              return broker.to_v3();
          });
    }

    raft::group_nodes current;
    std::optional<raft::group_nodes> old;

    if (likely(version >= raft::group_configuration::version_t(2))) {
        current = adl<raft::group_nodes>{}.from(p);
        old = adl<std::optional<raft::group_nodes>>{}.from(p);
    } else {
        // no raft::vnodes
        auto current_v0 = adl<group_nodes_v0>{}.from(p);
        auto old_v0 = adl<std::optional<group_nodes_v0>>{}.from(p);

        current = current_v0.to_v2();
        if (old_v0) {
            old = old_v0->to_v2();
        }
    }
    model::revision_id revision = raft::no_revision;
    if (version > raft::group_configuration::version_t(0)) {
        revision = adl<model::revision_id>{}.from(p);
    }
    return raft::group_configuration(
      std::move(brokers), std::move(current), revision, std::move(old));
}

void adl<raft::vnode>::to(iobuf& buf, raft::vnode id) {
    serialize(buf, id.id(), id.revision());
}

raft::vnode adl<raft::vnode>::from(iobuf_parser& p) {
    auto id = adl<model::node_id>{}.from(p);
    auto rev = adl<model::revision_id>{}.from(p);
    return raft::vnode(id, rev);
}

} // namespace reflection
