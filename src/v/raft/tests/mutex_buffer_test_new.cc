#include "raft/mutex_buffer.h"
#include "test_utils/fixture.h"
#include "utils/mutex.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/range/irange.hpp>
#include <fmt/core.h>

#include <cstddef>
#include <vector>

struct request {
    ss::sstring content;
};

struct response {
    ss::sstring content;
};

struct fixture {
    boost::integer_range<size_t> range = boost::irange<size_t>(0, 10);
};
using namespace std::chrono_literals;

void print_res(const std::vector<size_t>& res) {
    std::cout << "[";
    for (auto r : res) {
        std::cout << r;
    }
    std::cout << "]" << std::endl;
}

FIXTURE_TEST(parallel_for_each, fixture) {
    std::vector<size_t> res;
    ss::parallel_for_each(range, [&res](size_t i) {
        return ss::sleep(100ms).then([i, &res] { res.push_back(i); });
    }).get0();
    print_res(res);
}

FIXTURE_TEST(for_each, fixture) {
    std::vector<size_t> res;
    ss::do_for_each(range, [&res](size_t i) {
        return ss::sleep(100ms).then([i, &res] { res.push_back(i); });
    }).get0();
    print_res(res);
}

ss::future<>
in_c(boost::integer_range<size_t>& range, std::vector<size_t>& res) {
    for (auto i : range) {
        co_await ss::sleep(100ms).then([i, &res] { res.push_back(i); });
    }

    co_return;
}
FIXTURE_TEST(corutine, fixture) {
    std::vector<size_t> res;
    in_c(range, res).get0();
    print_res(res);
}
