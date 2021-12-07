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

#include "model/metadata.h"
#include "pandoscope/configuration.h"
#include "pandoscope/executor.h"
#include "storage/api.h"

#include <seastar/core/app-template.hh>
#include <seastar/util/defer.hh>

namespace po = boost::program_options; // NOLINT

class application {
public:
    int run(int, char**);

    ss::future<> initialize(pandoscope::configuration);

    explicit application(ss::sstring = "pandoscope-app");

private:
    void init_env();
    ss::app_template::config create_app_config();
    void add_program_options(ss::app_template&);
    ss::logger _log;

    std::unique_ptr<pandoscope::executor> _executor;
};
