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

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
/**
 * foreign promise utility allow cross core promise notification.
 */
template<typename T = void>
class foreign_promise {
public:
    template<typename... A>
    ss::future<> dispatch_set_value(A... v) && {
        return ss::smp::submit_to(
          _source_shard,
          [promise = std::move(_promise),
           ... v = std::forward<A>(v)]() mutable {
              promise->set_value(std::forward<A>(v)...);
          });
    }

    ss::future<> dispatch_set_exception(std::exception_ptr e) && {
        return ss::smp::submit_to(
          _source_shard, [promise = std::move(_promise), e]() mutable {
              promise->set_exception(e);
          });
    }

    ss::future<T> get_future() { return _promise->get_future(); }

private:
    ss::shard_id _source_shard = ss::this_shard_id();
    ss::foreign_ptr<std::unique_ptr<ss::promise<T>>> _promise
      = ss::make_foreign(std::make_unique<ss::promise<T>>());
};
