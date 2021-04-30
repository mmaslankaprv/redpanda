// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "seastarx.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

struct backlog_controller_config {
    backlog_controller_config(
      double kp,
      double ki,
      double kd,
      int setpoint,
      int initial_shares,
      std::chrono::milliseconds sampling_interval,
      ss::scheduling_group sg,
      const ss::io_priority_class& iopc,
      int min_shares,
      int max_shares);

    double proportional_coeff;
    double integral_coeff;
    double derivative_coeff;
    int setpoint;
    int initial_shares;
    std::chrono::milliseconds sampling_interval;
    ss::scheduling_group scheduling_group;
    const ss::io_priority_class& io_priority;
    int min_shares;
    int max_shares;
};

class backlog_controller {
public:
    struct sampler {
        virtual ss::future<int64_t> sample_backlog() = 0;
        virtual ~sampler() noexcept = default;
    };
    backlog_controller(
      std::unique_ptr<sampler>, ss::logger&, backlog_controller_config);

    void update_setpoint(int64_t);
    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<> set();
    ss::future<> update();

    std::unique_ptr<sampler> _sampler;
    ss::logger& _log;
    double _kp; // controller 'gain' -  proportional coefficient ;
    double _ki; // integral part coefficient ;
    double _kd; // derivative part coefficient ;
    std::chrono::milliseconds _sampling_interval;
    ss::timer<> _sampling_timer;
    // controlled resources
    ss::scheduling_group _scheduling_group;
    const ss::io_priority_class& _io_priority;
    // state
    std::array<int64_t, 2> _error_samples = {0, 0};
    int64_t _setpoint;
    int _current_shares;
    int _min_shares;
    int _max_shares;
    ss::gate _gate;
};
