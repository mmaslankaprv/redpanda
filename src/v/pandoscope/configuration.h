#pragma once

#include "model/record_batch_types.h"

#include <seastar/core/sstring.hh>

#include <boost/program_options/variables_map.hpp>

#include <filesystem>
#include <optional>
#include <ostream>
#include <vector>
#pragma once
#include "model/fundamental.h"

struct configuration {
    std::filesystem::path data_dir;

    bool summary;
    // log
    std::optional<model::ns> ns;
    std::optional<model::topic> topic;
    std::optional<model::partition_id> partition;

    // offsets
    model::offset begin;
    model::offset end;

    // type_filter
    std::vector<model::record_batch_type> batch_types;

    ss::sstring format;

    static configuration
    from_program_options(const boost::program_options::variables_map&);

    friend std::ostream& operator<<(std::ostream&, const configuration&);
};
