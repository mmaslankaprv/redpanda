#include "raft/append_entries_buffer.h"

#include "raft/consensus.h"
#include "raft/types.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>
#include <seastar/util/variant_utils.hh>

#include <exception>
#include <variant>
#include <vector>
namespace raft {

/**
 * This class allow to buffer arguments of a callers waiting for mutex and
 * when mutex is eventually available execute all buffered calls without
 * releasing mutex in beetween single calls.
 */

append_entries_buffer::append_entries_buffer(
  consensus& c, size_t max_buffered_elements)
  : _consensus(c)
  , _max_buffered(max_buffered_elements) {}

ss::future<append_entries_reply>
append_entries_buffer::enqueue(append_entries_request&& r) {
    return ss::with_gate(_gate, [this, r = std::move(r)]() mutable {
        // we do not use a semaphore as we do not want to wait, waiting
        // may cause requests reordering
        if (_requests.size() >= _max_buffered) {
            return ss::make_exception_future<append_entries_reply>(
              std::overflow_error("max buffered entries count reached."));
        }
        ss::promise<append_entries_reply> p;
        auto f = p.get_future();
        _requests.push_back(std::move(r));
        _responsens.push_back(std::move(p));
        _enequeued.signal();
        return f;
    });
}

ss::future<> append_entries_buffer::stop() {
    _enequeued.broken();
    return _gate.close();
}

void append_entries_buffer::start() {
    (void)ss::with_gate(_gate, [this]() mutable {
        return ss::do_until(
          [this] { return _gate.is_closed(); },
          [this]() mutable {
              return _enequeued.wait([this] { return !_requests.empty(); })
                .then([this]() mutable { return flush(); })
                .handle_exception_type(
                  [](const ss::broken_condition_variable&) {
                      // ignore exception, we are about to stop
                  });
          });
    });
}

ss::future<> append_entries_buffer::flush() {
    auto requests = std::exchange(_requests, {});
    auto response_promises = std::exchange(_responsens, {});

    return _consensus._op_lock.with(
      [this,
       requests = std::move(requests),
       response_promises = std::move(response_promises)]() mutable {
          return do_flush(std::move(requests), std::move(response_promises));
      });
}

ss::future<> append_entries_buffer::do_flush(
  request_t requests, response_t response_promises) {
    std::vector<reply_t> replies;
    replies.reserve(requests.size());
    return ss::do_with(
      std::move(requests),
      std::move(replies),
      bool{false},
      [this, response_promises = std::move(response_promises)](
        request_t& requests,
        std::vector<reply_t>& replies,
        bool& needs_flush) mutable {
          return ss::do_for_each(
                   requests,
                   [this, &replies, &needs_flush](append_entries_request& req) {
                       if (req.flush) {
                           needs_flush = true;
                       }
                       return _consensus.do_append_entries(std::move(req))
                         .then([&replies](append_entries_reply r) {
                             replies.emplace_back(r);
                         })
                         .handle_exception([&replies](std::exception_ptr e) {
                             replies.emplace_back(std::move(e));
                         });
                   })
            .then([this, &needs_flush] {
                if (needs_flush) {
                    return _consensus.flush_log();
                }
                return ss::now();
            })
            .then([this,
                   &replies,
                   response_promises = std::move(response_promises)]() mutable {
                propagate_results(replies, std::move(response_promises));
            });
      });
}

void append_entries_buffer::propagate_results(
  std::vector<reply_t>& replies, response_t response_promises) {
    auto resp_it = response_promises.begin();
    for (auto& reply : replies) {
        auto lstats = _consensus._log.offsets();
        ss::visit(
          reply,
          [&resp_it, &lstats](append_entries_reply r) {
              // this is important, we want to update response committed offset
              // here as we flushed after the response structure was created
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
