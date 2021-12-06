#include "pandoscope/configuration.h"

#include "model/fundamental.h"

#include <fmt/ostream.h>

configuration configuration::from_program_options(
  const boost::program_options::variables_map& variables) {
    configuration ret;
    ret.data_dir = std::filesystem::path(
      variables["data-dir"].as<std::string>());
    ret.summary = variables["summary"].as<bool>();
    ret.ns = model::ns(variables["namespace"].as<std::string>());
    ret.topic = model::topic(variables["topic"].as<std::string>());
    ret.partition = model::partition_id(variables["partition"].as<int>());
    return ret;
}

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(o, "{{data_directory: {}}}", cfg.data_dir);
    return o;
}
