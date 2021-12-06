// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandoscope/application.h"

#include "model/fundamental.h"
#include "model/metadata.h"
#include "pandoscope/configuration.h"
#include "pandoscope/transform.h"
#include "storage/api.h"
#include "storage/batch_cache.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/types.h"
#include "utils/directory_walker.h"
#include "version.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/later.hh>

#include <boost/lexical_cast.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <sys/utsname.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <regex>
#include <stdexcept>
#include <string>
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
                auto deferred = ss::defer([this] {
                    auto deferred = std::move(_deferred);
                    // stop services in reverse order
                    while (!deferred.empty()) {
                        deferred.pop_back();
                    }
                });
                auto app_cfg = configuration::from_program_options(cfg);
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

storage::log_config make_log_config(const configuration& cfg) {
    return storage::log_config(
      storage::log_config::storage_type::disk,
      cfg.data_dir.c_str(),
      1024_MiB,
      1024_MiB,
      1024_MiB,
      storage::debug_sanitize_files::yes,
      ss::default_priority_class(),
      std::nullopt,
      std::chrono::hours(24),
      std::chrono::hours::max(),
      storage::with_cache::no,
      storage::batch_cache::reclaim_options(),
      std::chrono::hours(24),
      ss::default_scheduling_group());
}

storage::kvstore_config make_kvstore_config(const configuration& cfg) {
    using namespace std::chrono_literals;

    return storage::kvstore_config(
      1024_MiB, 1s, cfg.data_dir.c_str(), storage::debug_sanitize_files::yes);
}

ss::future<std::vector<std::pair<model::ntp, model::revision_id>>>
build_ntp_list(std::filesystem::path root) {
    std::vector<std::pair<model::ntp, model::revision_id>> ntps;
    static std::regex partition_dir_pattern(R"((\d+)_(\d+))");
    // first level, namespace
    co_await directory_walker::walk(
      root.string(), [&ntps, root](ss::directory_entry namespace_e) {
          if (namespace_e.type != ss::directory_entry_type::directory) {
              return ss::now();
          }
          auto ns_path = root / namespace_e.name.c_str();

          return directory_walker::walk(
            ns_path.string(),
            [&ntps, ns = namespace_e.name, ns_path](
              ss::directory_entry topic_e) {
                if (topic_e.type != ss::directory_entry_type::directory) {
                    return ss::now();
                }
                auto tp_path = ns_path / topic_e.name.c_str();
                return directory_walker::walk(
                  tp_path.string(),
                  [&ntps, ns, tp = topic_e.name](
                    ss::directory_entry partition_e) {
                      if (
                        partition_e.type
                        != ss::directory_entry_type::directory) {
                          return ss::now();
                      }
                      std::cmatch matches;
                      auto match = std::regex_search(
                        partition_e.name.c_str(),
                        matches,
                        partition_dir_pattern);
                      if (!match) {
                          return ss::now();
                      }

                      ntps.emplace_back(
                        model::ntp(
                          model::ns(ns),
                          model::topic(tp),
                          model::partition_id(
                            boost::lexical_cast<int32_t>(matches[1]))),
                        model::revision_id(
                          boost::lexical_cast<int64_t>(matches[2])));
                      return ss::now();
                  });
            });
      });
    co_return ntps;
};

storage::ntp_config make_ntp_config(
  model::ntp ntp, const ss::sstring& data_dir, model::revision_id rev) {
    auto overrides = std::make_unique<storage::ntp_config::default_overrides>();
    overrides->cleanup_policy_bitflags = model::cleanup_policy_bitflags::none;

    return storage::ntp_config(
      std::move(ntp), data_dir, std::move(overrides), rev);
}

ss::future<> application::initialize(configuration cfg) {
    _storage = std::make_unique<storage::api>(
      make_kvstore_config(cfg), make_log_config(cfg));

    co_await _storage->start();
    _deferred.emplace_back([this] { _storage->stop().get(); });

    _available_ntps = co_await build_ntp_list(cfg.data_dir);

    for (auto& [n, r] : _available_ntps) {
        vlog(_log.info, "found {}", n);
    }
    model::ntp ntp(*cfg.ns, *cfg.topic, *cfg.partition);

    auto it = std::find_if(
      _available_ntps.begin(), _available_ntps.end(), [&ntp](auto& p) {
          return p.first == ntp;
      });

    if (it == _available_ntps.end()) {
        vlog(
          _log.error,
          "unable to find ntp: {} in data folder: {}",
          ntp,
          cfg.data_dir);
        throw std::invalid_argument("NTP not found");
    }

    try {
        auto log = co_await _storage->log_mgr().manage(
          make_ntp_config(it->first, cfg.data_dir.string(), it->second));
        auto dst_ntp = it->first;
        dst_ntp.ns = model::ns(it->first.ns() + "_res");
        auto dst_cfg = make_ntp_config(
          dst_ntp, cfg.data_dir.string(), it->second);
        auto dst = co_await _storage->log_mgr().manage(std::move(dst_cfg));
        // std::vector<pandoscope::transformation> trs;
        // trs.push_back(pandoscope::make_printing_transform());
        // co_await pandoscope::transform(log, dst, std::move(trs));
        co_await pandoscope::print(log, pandoscope::make_simple_printer());

    } catch (const std::exception_ptr& e) {
        vlog(_log.error, "unable to read ntp: {} - error {}", ntp, e);
    }
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
