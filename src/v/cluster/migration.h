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
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

namespace cluster {

class migration {
public:
    struct context {
        ss::gate& gate;
        ss::abort_source& abort_source;
    };

    struct impl {
        virtual bool is_required() = 0;
        virtual ss::future<> apply(context) = 0;
        virtual std::string_view name() const = 0;
        virtual ~impl() = default;
    };

    explicit migration(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    bool is_required() { return _impl->is_required(); }

    ss::future<> apply(context ctx) { return _impl->apply(ctx); }

    std::string_view name() { return _impl->name(); }

private:
    std::unique_ptr<impl> _impl;
};

} // namespace cluster
