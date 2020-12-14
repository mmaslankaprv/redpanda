#pragma once

#include "prometheus/prometheus_sanitize.h"
#include "utils/hdr_hist.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>

#include <fmt/core.h>

#include <iostream>
#include <iterator>
#include <string_view>

class hist_helper {
public:
    explicit hist_helper(std::string_view name)
      : _log(fmt::format("h-{}-{}", name, ss::this_shard_id())) {
        _helpers.push_back(this);
        if (!_printing_timer.armed()) {
            using namespace std::chrono_literals;
            _printing_timer.set_callback([] {
                for (auto h : _helpers) {
                    vlog(h->_log.info, "hist: {}", h->_hist);
                }

                for (auto h : _helpers) {
                    h->_hist = hdr_hist{};
                    vlog(h->_log.info, "hist: clear");
                }
            });
            _printing_timer.arm_periodic(5s);
        }
    }

    auto measure(ss::future<>&& f) {
        return f.then([m = _hist.auto_measure()] {});
    }

    template<typename T>
    auto measure(ss::future<T>&& f) {
        return f.then([m = _hist.auto_measure()](auto ret) { return ret; });
    }

    template<typename Func>
    auto measure_f(Func&& f) {
        auto fut = ss::futurize_invoke(std::forward<Func>(f))
                     .then([m = _hist.auto_measure()](auto r) { return r; });
        return fut;
    }

    hdr_hist& get_hist() { return _hist; }

private:
    hdr_hist _hist;
    static thread_local ss::timer<> _printing_timer;
    static thread_local ss::timer<> _reset_timer;
    static thread_local int _cnt;
    ss::logger _log;
    ss::metrics::metric_groups _metrics;
    static thread_local std::vector<hist_helper*> _helpers;
};
