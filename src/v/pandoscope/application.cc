// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandoscope/application.h"

#include "net/unresolved_address.h"
#include "pandoscope/executor.h"
#include "pandoscope/offset_translator.h"
#include "pandoscope/print.h"
#include "pandoscope/transform.h"
#include "version.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <boost/lexical_cast.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <sys/utsname.h>

#include <vector>

application::application(ss::sstring logger_name)
  : _log(std::move(logger_name)) {}

int application::run(int ac, char** av) {
    init_env();
    vlog(_log.info, "Pandoscope {}", redpanda_version());
    struct ::utsname buf;
    ::uname(&buf);
    vlog(
      _log.info,
      "kernel={}, nodename={}, machine={}",
      buf.release,
      buf.nodename,
      buf.machine);
    ss::app_template app(create_app_config());
    add_program_options(app);
    return app.run(ac, av, [this, &app] {
        auto& cfg = app.configuration();

        return ss::async([this, &cfg] {
            try {
                auto app_cfg = pandoscope::configuration::from_program_options(
                  cfg);
                vlog(_log.info, "Configuration: {}", app_cfg);

                initialize(app_cfg).get();

                vlog(_log.info, "Stopping...");
            } catch (...) {
                vlog(
                  _log.info,
                  "Failure during startup: {}",
                  std::current_exception());
                return 1;
            }
            return 0;
        });
    });
}

void application::init_env() { std::setvbuf(stdout, nullptr, _IOLBF, 1024); }

ss::future<> application::initialize(pandoscope::configuration cfg) {
    try {
        _executor = std::make_unique<pandoscope::executor>(std::move(cfg));
        co_await _executor->start();

        pandoscope::address_mapping mapping;
        mapping.kafka.emplace(
          net::unresolved_address(
            "rp-be4083ec48c0708970213f46f848576ab55fd1c3-0.rp-"
            "be4083ec48c0708970213f46f848576ab55fd1c3.rp-37-58-139.svc.cluster."
            "local.",
            9092),
          net::unresolved_address("172.31.11.76", 9092));
        mapping.kafka.emplace(
          net::unresolved_address(
            "rp-be4083ec48c0708970213f46f848576ab55fd1c3-1.rp-"
            "be4083ec48c0708970213f46f848576ab55fd1c3.rp-37-58-139.svc.cluster."
            "local.",
            9092),
          net::unresolved_address("172.31.14.150", 9092));
        mapping.kafka.emplace(
          net::unresolved_address(
            "rp-be4083ec48c0708970213f46f848576ab55fd1c3-2.rp-"
            "be4083ec48c0708970213f46f848576ab55fd1c3.rp-37-58-139.svc.cluster."
            "local.",
            9092),
          net::unresolved_address("172.31.11.150", 9092));

        mapping.rpc.emplace(
          net::unresolved_address(
            "rp-be4083ec48c0708970213f46f848576ab55fd1c3-0.rp-"
            "be4083ec48c0708970213f46f848576ab55fd1c3.rp-37-58-139.svc.cluster."
            "local.",
            33145),
          net::unresolved_address("172.31.11.76", 33145));
        mapping.rpc.emplace(
          net::unresolved_address(
            "rp-be4083ec48c0708970213f46f848576ab55fd1c3-1.rp-"
            "be4083ec48c0708970213f46f848576ab55fd1c3.rp-37-58-139.svc.cluster."
            "local.",
            33145),
          net::unresolved_address("172.31.14.150", 33145));
        mapping.rpc.emplace(
          net::unresolved_address(
            "rp-be4083ec48c0708970213f46f848576ab55fd1c3-2.rp-"
            "be4083ec48c0708970213f46f848576ab55fd1c3.rp-37-58-139.svc.cluster."
            "local.",
            33145),
          net::unresolved_address("172.31.11.150", 33145));

        std::vector<pandoscope::transformation> tr;
        tr.push_back(
          pandoscope::make_cluster_config_transformation(std::move(mapping)));
        co_await _executor->execute(
          pandoscope::transform_command(std::move(tr)));

    } catch (const std::exception_ptr& e) {
        vlog(_log.error, "unable to start pandoscope - {}", e);
    }
    co_await _executor->stop();
}

void application::add_program_options(ss::app_template& app) {
    auto o = app.add_options();
    o("data-dir",
      po::value<std::string>()->default_value("/var/lib/redpanda"),
      "redpanda data directory");

    o("namespace", po::value<std::string>(), "namespace");
    o("topic", po::value<std::string>(), "topic");
    o("partition", po::value<int32_t>(), "partition");

    o("start_offset", po::value<int64_t>(), "first printed offset");
    o("end_offset", po::value<int64_t>(), "last printed offset");
    o("summary", po::value<bool>()->default_value(false), "print summary only");
    o("command", po::value<std::string>(), "what to do");
}

ss::app_template::config application::create_app_config() {
    ss::app_template::config app_cfg;
    app_cfg.name = "Pandoscope 🔍";
    using namespace std::literals::chrono_literals; // NOLINT
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    return app_cfg;
}
