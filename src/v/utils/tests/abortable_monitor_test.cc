// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/abortable_monitor.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
// The async_transform* tests also implicity tests that invoked callbacks occur
// in lock step. Since the implementation pushes back the result as soon as the
// future becomes available, if this mechanism was working incorrectly, the
// result array would appear to have out of order elements
SEASTAR_THREAD_TEST_CASE(test_simple) {
    abortable_monitor<int> monitor;
    auto f_1 = monitor.wait();
    auto f_2 = monitor.wait();
    monitor.notify(10);
    monitor.notify(11);
    BOOST_REQUIRE_EQUAL(f_1.get(), 10);
    BOOST_REQUIRE_EQUAL(f_2.get(), 11);
}

SEASTAR_THREAD_TEST_CASE(test_not_missing_updates) {
    abortable_monitor<int> monitor;
    monitor.notify(10);
    monitor.notify(11);
    monitor.notify(13);
    auto f_1 = monitor.wait();
    BOOST_REQUIRE_EQUAL(f_1.get(), 10);
    auto f_2 = monitor.wait();
    BOOST_REQUIRE_EQUAL(f_2.get(), 11);
    auto f_3 = monitor.wait();
    BOOST_REQUIRE_EQUAL(f_3.get(), 13);
}

SEASTAR_THREAD_TEST_CASE(test_abort) {
    ss::abort_source as;
    abortable_monitor<int> monitor;
    auto f_1 = monitor.wait(as);
    auto f_2 = monitor.wait(as);
    as.request_abort();

    BOOST_REQUIRE_THROW(f_1.get(), ss::abort_requested_exception);
    BOOST_REQUIRE_THROW(f_2.get(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(test_abort_individual) {
    ss::abort_source as;
    abortable_monitor<int> monitor;
    auto f_1 = monitor.wait(as);
    auto f_2 = monitor.wait();
    as.request_abort();

    BOOST_REQUIRE_THROW(f_1.get(), ss::abort_requested_exception);
    monitor.notify(10);
    BOOST_REQUIRE_EQUAL(f_2.get(), 10);
}

SEASTAR_THREAD_TEST_CASE(test_stopping) {
    ss::abort_source as;
    abortable_monitor<int> monitor;
    auto f_1 = monitor.wait();
    auto f_2 = monitor.wait();
    monitor.stop();

    BOOST_REQUIRE_THROW(f_1.get(), ss::abort_requested_exception);
    BOOST_REQUIRE_THROW(f_2.get(), ss::abort_requested_exception);
}
