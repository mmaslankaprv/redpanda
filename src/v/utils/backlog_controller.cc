// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "utils/backlog_controller.h"

#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <chrono>

backlog_controller_config::backlog_controller_config(
  double kp,
  double ki,
  double kd,
  int sp,
  int initial_shares,
  std::chrono::milliseconds interval,
  ss::scheduling_group sg,
  const ss::io_priority_class& iop,
  int min,
  int max)
  : proportional_coeff(kp)
  , integral_coeff(ki)
  , derivative_coeff(kd)
  , setpoint(sp)
  , initial_shares(initial_shares)
  , sampling_interval(interval)
  , scheduling_group(sg)
  , io_priority(iop)
  , min_shares(min)
  , max_shares(max) {}

backlog_controller::backlog_controller(
  std::unique_ptr<sampler> sampler,
  ss::logger& logger,
  backlog_controller_config cfg)
  : _sampler(std::move(sampler))
  , _log(logger)
  , _kp(cfg.proportional_coeff)
  , _ki(cfg.integral_coeff)
  , _kd(cfg.derivative_coeff)
  , _sampling_interval(cfg.sampling_interval)
  , _scheduling_group(cfg.scheduling_group)
  , _io_priority(cfg.io_priority)
  , _setpoint(cfg.setpoint)
  , _current_shares(cfg.initial_shares)
  , _min_shares(cfg.min_shares)
  , _max_shares(cfg.max_shares) {}

ss::future<> backlog_controller::start() {
    _sampling_timer.set_callback([this] {
        (void)ss::with_gate(_gate, [this] {
            return update().then([this] {
                if (!_gate.is_closed()) {
                    _sampling_timer.arm(_sampling_interval);
                }
            });
        });
    });
    _sampling_timer.arm(_sampling_interval);
    return ss::now();
}

void backlog_controller::update_setpoint(int64_t v) {
    vlog(_log.info, "new setpoint value: {}", v);
    _setpoint = v;
}

ss::future<> backlog_controller::stop() {
    if (_sampling_timer.armed()) {
        _sampling_timer.cancel();
    }
    return _gate.close();
}

ss::future<> backlog_controller::update() {
    auto current_backlog = co_await _sampler->sample_backlog();
    auto new_error = _setpoint - current_backlog;
    /**
     * We use
     */
    auto update = (new_error - _error_samples[0]) + _ki * new_error
                  + _kd
                      * (new_error - 2 * _error_samples[0] + _error_samples[1]);
    update *= _kp;
    /**
     *
     */
    _current_shares += static_cast<int>(update);
    _current_shares = std::min(_current_shares, _max_shares);
    _current_shares = std::max(_current_shares, _min_shares);
    vlog(
      _log.trace,
      "state update: {{setpoint: {}, current_backlog: {}, current_error: {}, "
      "prev_error: {}, shares_update: {}, current_share: {} }}",
      _setpoint,
      current_backlog,
      new_error,
      _error_samples[0],
      -update,
      _current_shares);
    // update error samples
    _error_samples[1] = _error_samples[0];
    _error_samples[0] = new_error;

    co_await set();
}

ss::future<> backlog_controller::set() {
    vlog(_log.debug, "updating shares {}", _current_shares);
    _scheduling_group.set_shares(static_cast<float>(_current_shares));
    co_await ss::engine().update_shares_for_class(
      _io_priority, _current_shares);
}
