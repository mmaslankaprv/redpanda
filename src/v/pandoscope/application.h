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
#include "storage/api.h"

#include <seastar/core/app-template.hh>
#include <seastar/util/defer.hh>

#include <memory>

namespace po = boost::program_options; // NOLINT

class application {
public:
    int run(int, char**);

    ss::future<> initialize(configuration);

    explicit application(ss::sstring = "pandoscope");

    void shutdown() {
        while (!_deferred.empty()) {
            _deferred.pop_back();
        }
    }

private:
    void init_env();
    ss::app_template::config create_app_config();
    void add_program_options(ss::app_template&);

    using deferred_actions
      = std::vector<ss::deferred_action<std::function<void()>>>;

    ss::logger _log;

    // run these first on destruction
    deferred_actions _deferred;
    std::unique_ptr<storage::api> _storage;
    std::vector<std::pair<model::ntp, model::revision_id>> _available_ntps;
};
