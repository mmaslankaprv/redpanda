#include "raft/append_entries_buffer.h"

#include "raft/consensus.h"
#include "raft/types.h"
#include "utils/hist_helper.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>

#include <chrono>
#include <cstdint>
#include <exception>
#include <variant>
#include <vector>
namespace raft {

append_entries_buffer::append_entries_buffer(
  consensus& c, size_t max_buffered_elements)
  : _consensus(c)
  , _max_buffered(max_buffered_elements) {
    std::cout << _max_buffered << std::endl;
}

ss::future<append_entries_reply>
append_entries_buffer::enqueue(append_entries_request&& r) {
    return ss::with_gate(_gate, [this, r = std::move(r)]() mutable {
        // we normally do not want to wait as it would cause requests
        // reordering. Reordering may only happend if we would wait on condition
        // variable.

        ss::promise<append_entries_reply> p;
        auto f = p.get_future();
        _requests.push_back(std::move(r));
        _responses.push_back(std::move(p));
        _enqueued.broadcast();
        return f;
    });
}

ss::future<> append_entries_buffer::stop() {
    auto f = _gate.close();
    _enqueued.broken();
    auto response_promises = std::exchange(_responses, {});
    // set errors
    for (auto& p : response_promises) {
        p.set_exception(ss::gate_closed_exception());
    }
    vassert(
      _responses.empty(),
      "response promises queue should be empty when append entries buffer is "
      "about to stop");

    return f;
}

struct tracker {
    uint64_t wait{0};
    uint64_t flush{0};

    uint64_t do_flush{0};
    uint64_t op_lock{0};
    uint64_t append_entries{0};
    uint64_t flush_log{0};
};

static thread_local tracker tr;
void append_entries_buffer::start() {
    static thread_local bool started = false;
    if (!started) {
        static thread_local ss::timer timer;
        timer.set_callback([] {
            vlog(
              raftlog.info,
              "DBG: wait: {}, flush: {}, do_flush: {}, op_lock: {}, "
              "append_entries: {}, flush_log: {}",
              tr.wait,
              tr.flush,
              tr.do_flush,
              tr.op_lock,
              tr.append_entries,
              tr.flush_log);
        });
        timer.arm_periodic(std::chrono::seconds(2));
        started = true;
    }

    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _gate.is_closed(); },
          [this] {
              tr.wait++;
              return _enqueued.wait([this] { return !_requests.empty(); })
                .then([this] {
                    static thread_local hist_helper h{"raft-buffer-flush"};
                    auto m = h.auto_measure();
                    tr.flush++;
                    return flush().finally([m = std::move(m)] { tr.flush--; });
                })
                .handle_exception([](const std::exception_ptr&) {
                    // ignore exception, we are about to stop
                })
                .finally([] { tr.wait--; });
          });
    });
}

ss::future<> append_entries_buffer::flush() {
    static thread_local hist_helper h{"raft-buffer-cnt"};
    // empty requests, do nothing
    if (_requests.empty()) {
        return ss::now();
    }
    auto requests = std::exchange(_requests, {});
    auto response_promises = std::exchange(_responses, {});
    h.get_hist().record(requests.size());
    tr.op_lock++;
    return _consensus._op_lock.get_units().then(
      [this,
       requests = std::move(requests),
       response_promises = std::move(response_promises)](
        ss::semaphore_units<> u) mutable {
          tr.op_lock--;
          tr.do_flush++;
          return do_flush(
                   std::move(requests),
                   std::move(response_promises),
                   std::move(u))
            .finally([] { tr.do_flush--; });
      });
}

ss::future<> append_entries_buffer::do_flush(
  request_t requests, response_t response_promises, ss::semaphore_units<> u) {
    bool needs_flush = false;
    std::vector<reply_t> replies;
    auto f = ss::now();

    replies.reserve(requests.size());
    for (auto& req : requests) {
        if (req.flush) {
            needs_flush = true;
        }
        try {
            // NOTE: do_append_entries do not flush
            tr.append_entries++;
            auto reply = co_await _consensus.do_append_entries(std::move(req));
            replies.emplace_back(reply);
            tr.append_entries--;
        } catch (...) {
            replies.emplace_back(std::current_exception());
            tr.append_entries--;
        }
    }
    if (needs_flush) {
        tr.flush_log++;
        f = _consensus.flush_log();
        u.return_all();
        co_await std::move(f);
        tr.flush_log--;
    }
    // units were released before flushing log

    propagate_results(std::move(replies), std::move(response_promises));
    co_return;
}

void append_entries_buffer::propagate_results(
  std::vector<reply_t> replies, response_t response_promises) {
    vassert(replies.size() == response_promises.size(), "size mismatch");
    auto resp_it = response_promises.begin();
    auto lstats = _consensus._log.offsets();
    for (auto& reply : replies) {
        ss::visit(
          reply,
          [&resp_it, &lstats](append_entries_reply r) {
              // this is important, we want to update response committed
              // offset here as we flushed after the response structure was
              // created
              r.last_committed_log_index = lstats.committed_offset;
              resp_it->set_value(r);
          },
          [&resp_it](std::exception_ptr& e) {
              resp_it->set_exception(std::move(e));
          });
        resp_it++;
    }
}
} // namespace raft
