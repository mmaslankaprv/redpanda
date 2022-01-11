#pragma once

#include "model/record.h"
#include "net/unresolved_address.h"
#include "pandoscope/executor.h"
#include "storage/log.h"

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <vector>
namespace pandoscope {

class transformation {
public:
    struct impl {
        virtual ss::future<model::record_batch> apply(model::record_batch&) = 0;
        virtual bool is_applicable(const model::record_batch&) = 0;
        virtual ~impl() = default;
    };

    ss::future<model::record_batch> apply(model::record_batch& batch) {
        return _impl->apply(batch);
    }

    bool is_applicable(const model::record_batch& b) {
        return _impl->is_applicable(b);
    }

    explicit transformation(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

private:
    std::unique_ptr<impl> _impl;
};

ss::future<> transform(storage::log, storage::log, std::vector<transformation>);

transformation make_topic_configuration_transformation();

struct address_mapping {
    using mapping_t
      = absl::flat_hash_map<net::unresolved_address, net::unresolved_address>;
    mapping_t kafka;
    mapping_t rpc;
};

transformation make_cluster_config_transformation(address_mapping);

executor::command transform_command(std::vector<transformation>);

} // namespace pandoscope
