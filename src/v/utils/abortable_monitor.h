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
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>

#include <deque>

/**
 * An `abortable_monitor` helper class is a simple utility to communicate
 * between fibers. Fiber that produces the messages calls `notify_waiters` while
 * message consuming fiber is calling `wait()`. Waiters are waiting for the
 * notifications in FIFO queue, after data are available each waiter is notified
 * with single event unitil events queue is empty. This way we can scale the
 * numbers of waiters.
 */
template<typename T>
class abortable_monitor {
public:
    using optional_abort_source_t
      = std::optional<std::reference_wrapper<ss::abort_source>>;

    ss::future<T> wait(optional_abort_source_t as = {}) {
        if (_stopped) {
            return ss::make_exception_future<T>(
              ss::abort_requested_exception{});
        }

        if (!_updates.empty()) {
            auto f = ss::make_ready_future<T>(_updates.front());
            _updates.pop_front();
            return f;
        }

        auto waiter = std::make_unique<abortable_waiter>(_next_id++);
        auto f = waiter->promise.get_future();
        if (as) {
            auto subscription = as->get().subscribe(
              [this, &promise = waiter->promise, id = waiter->id]() noexcept {
                  promise.set_exception(ss::abort_requested_exception{});
                  std::erase_if(
                    _waiters, [id](const std::unique_ptr<abortable_waiter>& w) {
                        return id == w->id;
                    });
              });

            if (!subscription) {
                // already aborted
                return ss::make_exception_future<T>(
                  ss::abort_requested_exception{});
            }
            waiter->subscription = std::move(*subscription);
        }
        _waiters.push_back(std::move(waiter));

        return f;
    }

    void notify(T t) {
        _updates.push_back(std::move(t));
        if (_waiters.empty()) {
            return;
        }
        auto waiter = std::move(_waiters.front());
        _waiters.pop_front();

        std::exchange(waiter->subscription, {});
        waiter->promise.set_value(std::move(_updates.front()));

        _updates.pop_front();
    }

    void stop() {
        _stopped = true;
        auto waiters = std::exchange(_waiters, {});
        for (auto& w : waiters) {
            std::exchange(w->subscription, {});
            w->promise.set_exception(ss::abort_requested_exception{});
        }
    }

private:
    using waiter_id = named_type<uint64_t, struct am_waiter_id_tag>;
    struct abortable_waiter {
        explicit abortable_waiter(waiter_id id)
          : id(id) {}
        waiter_id id;
        ss::promise<T> promise;
        ss::abort_source::subscription subscription;
    };
    std::deque<std::unique_ptr<abortable_waiter>> _waiters;
    waiter_id _next_id{0};
    bool _stopped{false};

    ss::circular_buffer<T> _updates;
};
