/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "bytes/iobuf.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "seastarx.h"
#include "utils/named_type.h"


namespace kafka {

/**
 * Group log metadata consistent with Kafka internal format
 */
struct group_metadata_key {
    static constexpr group_metadata_version version{2};
    kafka::group_id group_id;

    friend std::ostream& operator<<(std::ostream&, const group_metadata_key&);
    friend bool operator==(const group_metadata_key&, const group_metadata_key&)
      = default;
    static group_metadata_key decode(request_reader&);
    static void encode(response_writer&, const group_metadata_key&);
};

/**
 * Group log metadata consistent with Kafka internal format
 */
struct group_metadata_value {
    static constexpr group_metadata_version version{3};
    kafka::protocol_type protocol_type;
    kafka::generation_id generation;
    std::optional<kafka::protocol_name> protocol;
    std::optional<kafka::member_id> leader;
    model::timestamp state_timestamp;
    std::vector<member_state> members;

    group_metadata_value copy() const {
        group_metadata_value ret{
          .protocol_type = protocol_type,
          .generation = generation,
          .protocol = protocol,
          .leader = leader,
          .state_timestamp = state_timestamp,
        };
        ret.members.reserve(members.size());
        std::transform(
          members.begin(),
          members.end(),
          std::back_inserter(ret.members),
          [](const member_state& ms) { return ms.copy(); });

        return ret;
    }

    friend std::ostream& operator<<(std::ostream&, const group_metadata_value&);
    friend bool
    operator==(const group_metadata_value&, const group_metadata_value&)
      = default;

    static group_metadata_value decode(request_reader&);
    static void encode(response_writer&, const group_metadata_value&);
};

struct offset_metadata_key {
    static constexpr group_metadata_version version{1};
    kafka::group_id group_id;
    model::topic topic;
    model::partition_id partition;

    friend std::ostream& operator<<(std::ostream&, const offset_metadata_key&);
    friend bool
    operator==(const offset_metadata_key&, const offset_metadata_key&)
      = default;
    static offset_metadata_key decode(request_reader&);
    static void encode(response_writer&, const offset_metadata_key&);
};

/**
 * The value type for offset commit records, consistent with Kafka format
 */
struct offset_metadata_value {
    static constexpr group_metadata_version version{3};
    model::offset offset;
    kafka::leader_epoch leader_epoch = invalid_leader_epoch;
    ss::sstring metadata;
    model::timestamp commit_timestamp;
    model::timestamp expire_timestamp{-1};

    friend std::ostream&
    operator<<(std::ostream&, const offset_metadata_value&);
    friend bool
    operator==(const offset_metadata_value&, const offset_metadata_value&)
      = default;
    static offset_metadata_value decode(request_reader&);
    static void encode(response_writer&, const offset_metadata_value&);
};

struct offset_metadata_kv {
    offset_metadata_key key;
    std::optional<offset_metadata_value> value;
};

struct group_metadata_kv {
    group_metadata_key key;
    std::optional<group_metadata_value> value;
};

inline group_metadata_version read_metadata_version(request_reader& reader) {
    return group_metadata_version{reader.read_int16()};
}

namespace std {
template<>
struct hash<kafka::offset_metadata_key> {
    size_t operator()(const kafka::offset_metadata_key& key) const {
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(key.group_id));
        boost::hash_combine(h, hash<ss::sstring>()(key.topic));
        boost::hash_combine(h, hash<model::partition_id>()(key.partition));
        return h;
    }
};

} // namespace std
