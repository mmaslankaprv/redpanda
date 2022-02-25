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
namespace old {
/**
 * the key type for group membership log records.
 *
 * the opaque key field is decoded based on the actual type.
 *
 * TODO: The `noop` type indicates a control structure used to synchronize raft
 * state in a transition to leader state so that a consistent read is made. this
 * is a temporary work-around until we fully address consistency semantics in
 * raft.
 */
struct group_log_record_key {
    enum class type : int8_t { group_metadata, offset_commit, noop };

    type record_type;
    iobuf key;
};

/// \addtogroup kafka-groups
/// @{

/**
 * Member state.
 *
 * This structure is used in-memory at runtime to hold member state. It is also
 * serialized to stable storage to checkpoint group state, and is therefore
 * sensitive to change.
 */
struct member_state {
    kafka::member_id id;
    std::chrono::milliseconds session_timeout;
    std::chrono::milliseconds rebalance_timeout;
    std::optional<kafka::group_instance_id> instance_id;
    kafka::protocol_type protocol_type;
    std::vector<member_protocol> protocols;
    iobuf assignment;
    kafka::client_id client_id;
    kafka::client_host client_host;

    member_state copy() const {
        return member_state{
          .id = id,
          .session_timeout = session_timeout,
          .rebalance_timeout = rebalance_timeout,
          .instance_id = instance_id,
          .protocol_type = protocol_type,
          .protocols = protocols,
          .assignment = assignment.copy(),
          .client_id = client_id,
          .client_host = client_host,
        };
    }

    friend bool operator==(const member_state&, const member_state&) = default;
};
/**
 * the value type of a group metadata log record.
 */
struct group_log_group_metadata {
    kafka::protocol_type protocol_type;
    kafka::generation_id generation;
    std::optional<kafka::protocol_name> protocol;
    std::optional<kafka::member_id> leader;
    int32_t state_timestamp;
    std::vector<member_state> members;
};

/**
 * the key type for offset commit records.
 */
struct group_log_offset_key {
    kafka::group_id group;
    model::topic topic;
    model::partition_id partition;

    bool operator==(const group_log_offset_key& other) const = default;

    friend std::ostream& operator<<(std::ostream&, const group_log_offset_key&);
};

/**
 * the value type for offset commit records.
 */
struct group_log_offset_metadata {
    model::offset offset;
    int32_t leader_epoch;
    std::optional<ss::sstring> metadata;

    friend std::ostream&
    operator<<(std::ostream&, const group_log_offset_metadata&);
};
}; // namespace old
enum group_metadata_type {
    offset_commit,
    group_metadata,
    noop,
};

group_metadata_type decode_metadata_type(request_reader& key_reader);

using group_metadata_version = named_type<int16_t, struct md_versio_tag>;

struct member_state {
    static constexpr group_metadata_version version{3};
    kafka::member_id id;
    std::optional<group_instance_id> instance_id;
    kafka::client_id client_id;
    kafka::client_host client_host;
    std::chrono::milliseconds rebalance_timeout;
    std::chrono::milliseconds session_timeout;
    iobuf subscription;
    iobuf assignment;

    member_state copy() const {
        return member_state{
          .id = id,
          .instance_id = instance_id,
          .client_id = client_id,
          .client_host = client_host,
          .rebalance_timeout = rebalance_timeout,
          .session_timeout = session_timeout,
          .subscription = subscription.copy(),
          .assignment = assignment.copy(),
        };
    }
    friend std::ostream& operator<<(std::ostream&, const member_state&);

    friend bool operator==(const member_state&, const member_state&) = default;

    static member_state decode(request_reader&);
    static void encode(response_writer&, const member_state&);
};

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

class group_metadata_serializer {
public:
    struct key_value {
        iobuf key;
        std::optional<iobuf> value;
    };
    struct impl {
        virtual group_metadata_type get_metadata_type(iobuf) = 0;
        virtual key_value to_kv(group_metadata_kv) = 0;
        virtual key_value to_kv(offset_metadata_kv) = 0;

        virtual group_metadata_kv decode_group_metadata(model::record) = 0;
        virtual offset_metadata_kv decode_offset_metadata(model::record) = 0;
        virtual ~impl() = default;
    };

    explicit group_metadata_serializer(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    group_metadata_type get_metadata_type(iobuf buf) {
        return _impl->get_metadata_type(std::move(buf));
    };

    key_value to_kv(group_metadata_kv md) {
        return _impl->to_kv(std::move(md));
    }
    key_value to_kv(offset_metadata_kv md) {
        return _impl->to_kv(std::move(md));
    }

    group_metadata_kv decode_group_metadata(model::record record) {
        return _impl->decode_group_metadata(std::move(record));
    }

    offset_metadata_kv decode_offset_metadata(model::record record) {
        return _impl->decode_offset_metadata(std::move(record));
    }

private:
    std::unique_ptr<impl> _impl;
};
} // namespace kafka
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

namespace reflection {

/*
 * new code + new version: will see extra bytes for client host/id
 * new code + old version: will observe no extra bytes
 * old code + old/new version: will not read new appended fields
 *
 * on the wire we write out a version that is compatible with old code and then
 * append the additional client host/id information for each member. then when
 * deserializing we process the appended data by updating the old versions from
 * disk in the new in-memory structs which contain the extra member metadata.
 * old code skips over the appended data.
 */
template<>
struct adl<kafka::old::group_log_group_metadata> {
    struct member_state_v0 {
        kafka::member_id id;
        std::chrono::milliseconds session_timeout;
        std::chrono::milliseconds rebalance_timeout;
        std::optional<kafka::group_instance_id> instance_id;
        kafka::protocol_type protocol_type;
        std::vector<kafka::member_protocol> protocols;
        iobuf assignment;
    };

    struct group_log_group_metadata_v0 {
        kafka::protocol_type protocol_type;
        kafka::generation_id generation;
        std::optional<kafka::protocol_name> protocol;
        std::optional<kafka::member_id> leader;
        int32_t state_timestamp;
        std::vector<member_state_v0> members;
    };

    struct client_host_id {
        kafka::client_id id;
        kafka::client_host host;
    };

    void to(iobuf& out, kafka::old::group_log_group_metadata&& data) {
        // create instance of old version of members
        std::vector<member_state_v0> members_v0;
        members_v0.reserve(data.members.size());
        for (auto& member : data.members) {
            members_v0.push_back(member_state_v0{
              .id = std::move(member.id),
              .session_timeout = member.session_timeout,
              .rebalance_timeout = member.rebalance_timeout,
              .instance_id = std::move(member.instance_id),
              .protocol_type = std::move(member.protocol_type),
              .protocols = std::move(member.protocols),
              .assignment = std::move(member.assignment),
            });
        }

        // create instance of old version of group metadata
        group_log_group_metadata_v0 metadata_v0{
          .protocol_type = std::move(data.protocol_type),
          .generation = data.generation,
          .protocol = std::move(data.protocol),
          .leader = std::move(data.leader),
          .state_timestamp = data.state_timestamp,
          .members = std::move(members_v0),
        };

        // this puts a version on disk that older code can read
        serialize(out, std::move(metadata_v0));

        // now append the client host/id information for new code
        std::vector<client_host_id> client_info;
        client_info.reserve(data.members.size());
        for (auto& member : data.members) {
            client_info.push_back(client_host_id{
              .id = std::move(member.client_id),
              .host = std::move(member.client_host),
            });
        }
        serialize(out, std::move(client_info));
    }

    kafka::old::group_log_group_metadata from(iobuf_parser& in) {
        auto metadata_v0 = adl<group_log_group_metadata_v0>{}.from(in);

        std::vector<client_host_id> client_info;
        if (in.bytes_left()) {
            client_info = adl<std::vector<client_host_id>>{}.from(in);
            vassert(
              client_info.size() == metadata_v0.members.size(),
              "Expected client info size {} got {}",
              metadata_v0.members.size(),
              client_info.size());
        }

        std::vector<kafka::old::member_state> members_out;
        members_out.reserve(metadata_v0.members.size());

        for (auto& member : metadata_v0.members) {
            members_out.push_back(kafka::old::member_state{
              .id = std::move(member.id),
              .session_timeout = member.session_timeout,
              .rebalance_timeout = member.rebalance_timeout,
              .instance_id = std::move(member.instance_id),
              .protocol_type = std::move(member.protocol_type),
              .protocols = std::move(member.protocols),
              .assignment = std::move(member.assignment),
            });
        }

        for (size_t i = 0; i < client_info.size(); i++) {
            members_out[i].client_id = std::move(client_info[i].id);
            members_out[i].client_host = std::move(client_info[i].host);
        }

        kafka::old::group_log_group_metadata metadata_out{
          .protocol_type = std::move(metadata_v0.protocol_type),
          .generation = metadata_v0.generation,
          .protocol = std::move(metadata_v0.protocol),
          .leader = std::move(metadata_v0.leader),
          .state_timestamp = metadata_v0.state_timestamp,
          .members = std::move(members_out),
        };

        return metadata_out;
    }
};
} // namespace reflection
