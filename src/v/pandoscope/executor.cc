#include "pandoscope/executor.h"

#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "pandoscope/configuration.h"
#include "pandoscope/logger.h"
#include "pandoscope/utils.h"
#include "storage/api.h"
#include "utils/directory_walker.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/io_priority_class.hh>

#include <chrono>
#include <memory>
#include <regex>
namespace pandoscope {

executor::executor(configuration cfg)
  : _cfg(std::move(cfg)) {}

storage::log_config makelogger_config(const configuration& cfg) {
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
      1024_MiB,
      config::mock_binding<std::chrono::milliseconds>(10ms),
      cfg.data_dir.c_str(),
      storage::debug_sanitize_files::yes);
}

storage::kvstore_config
make_kvstore_config(const configuration& cfg, model::ntp ntp) {
    using namespace std::chrono_literals;

    return storage::kvstore_config(
      1024_MiB,
      config::mock_binding<std::chrono::milliseconds>(10ms),
      cfg.data_dir.c_str(),
      storage::debug_sanitize_files::yes,
      std::move(ntp));
}

ss::future<std::vector<ntp_revision>> executor::build_ntp_list() {
    std::vector<ntp_revision> ret;
    const std::filesystem::path root = _cfg.data_dir;
    static std::regex partition_dir_pattern(R"((\d+)_(\d+))");
    // first level, namespace
    co_await directory_walker::walk(
      root.string(), [&ret, root](ss::directory_entry namespace_e) {
          if (namespace_e.type != ss::directory_entry_type::directory) {
              return ss::now();
          }
          auto ns_path = root / namespace_e.name.c_str();

          return directory_walker::walk(
            ns_path.string(),
            [&ret, ns = namespace_e.name, ns_path](
              ss::directory_entry topic_e) {
                if (topic_e.type != ss::directory_entry_type::directory) {
                    return ss::now();
                }
                auto tp_path = ns_path / topic_e.name.c_str();
                return directory_walker::walk(
                  tp_path.string(),
                  [&ret, ns, tp = topic_e.name](
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

                      ret.emplace_back(
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
    co_return ret;
};

storage::ntp_config make_ntp_config(
  model::ntp ntp, const ss::sstring& data_dir, model::revision_id rev) {
    auto overrides = std::make_unique<storage::ntp_config::default_overrides>();
    overrides->cleanup_policy_bitflags = model::cleanup_policy_bitflags::none;

    return storage::ntp_config(
      std::move(ntp), data_dir, std::move(overrides), rev);
}

std::vector<ntp_revision>
executor::filter_ntps(std::vector<ntp_revision> ntps) {
    auto end = ntps.end();

    if (_cfg.ns) {
        end = std::stable_partition(
          ntps.begin(), end, [ns = *_cfg.ns](const ntp_revision& ntpr) {
              return ntpr.ntp.ns == ns;
          });
    }

    if (_cfg.topic) {
        end = std::stable_partition(
          ntps.begin(), end, [tp = *_cfg.topic](const ntp_revision& ntpr) {
              return ntpr.ntp.tp.topic == tp;
          });
    }

    if (_cfg.partition) {
        end = std::stable_partition(
          ntps.begin(), end, [p = *_cfg.partition](const ntp_revision& ntpr) {
              return ntpr.ntp.tp.partition == p;
          });
    }

    std::vector<ntp_revision> ret;
    ret.reserve(std::distance(ntps.begin(), end));
    std::move(ntps.begin(), end, std::back_inserter(ret));
    return ret;
}

ss::future<> executor::start() {
    _storage = std::make_unique<storage::api>(
      [this]() { return make_kvstore_config(_cfg); }, makelogger_config(_cfg));

    co_await _storage->start();
    auto raft0_log = co_await get_raft0_log();
    if (raft0_log) {
        co_await _topics.apply_all(co_await read_all_reader(*raft0_log));
    }

    std::vector<ntp_revision> all_ntps = co_await build_ntp_list();

    for (auto& ntpr : all_ntps) {
        vlog(logger.info, "Found {}@{}", ntpr.ntp, ntpr.revision);
        if (ntpr.ntp.tp.topic == model::kvstore_topic) {
            vlog(logger.info, "Starting {} kvstore", ntpr.ntp);
            _kvs.emplace_back(std::make_unique<storage::kvstore>(
              make_kvstore_config(_cfg, ntpr.ntp)));
            co_await _kvs.back()->start();
        }
    }

    _ntps = filter_ntps(std::move(all_ntps));
    if (_ntps.empty()) {
        vlog(logger.error, "No ntps match requested filter");
        throw std::runtime_error("No ntps match requested filter");
    }
}

ss::future<std::optional<storage::log>> executor::get_raft0_log() {
    try {
        co_return co_await _storage->log_mgr().manage(make_ntp_config(
          model::controller_ntp,
          _cfg.data_dir.string(),
          model::revision_id(0)));

    } catch (const std::exception_ptr& e) {
        vlog(logger.error, "controller log is not present - {}", e);
        co_return std::nullopt;
    }
}

ss::future<> executor::execute(executor::command cmd) {
    for (auto& ntp_rev : _ntps) {
        co_await do_execute(ntp_rev, cmd);
    }
}
ss::future<storage::log> executor::get_log(ntp_revision ntp_rev) {
    auto l = _storage->log_mgr().get(ntp_rev.ntp);
    if (l) {
        co_return *l;
    }

    co_return co_await _storage->log_mgr().manage(
      make_ntp_config(ntp_rev.ntp, _cfg.data_dir.string(), ntp_rev.revision));
}

ss::future<>
executor::do_execute(const ntp_revision& ntp_rev, executor::command cmd) {
    try {
        auto log = co_await get_log(ntp_rev);
        // auto dst_ntp = it->first;
        // dst_ntp.ns = model::ns(it->first.ns() + "_res");
        // auto dst_cfg = make_ntp_config(
        //   dst_ntp, cfg.data_dir.string(), it->second);
        // auto dst = co_await _storage->log_mgr().manage(std::move(dst_cfg));
        // std::vector<pandoscope::transformation> trs;
        // trs.push_back(pandoscope::make_printing_transform());
        // co_await pandoscope::transform(log, dst, std::move(trs));
        vlog(logger.error, "Executing {} over {}", cmd.name(), ntp_rev.ntp);
        co_await cmd.execute(log, get_ctx());
    } catch (const std::exception_ptr& e) {
        vlog(logger.error, "Error executing command - {}", e);
    }
}

ss::future<> executor::stop() {
    co_await _storage->stop();
    for (auto& kv : _kvs) {
        co_await kv->stop();
    }
}
} // namespace pandoscope
