#pragma once

#include "raft/types.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

namespace raft {

class consensus;

class append_entries_buffer {
public:
    explicit append_entries_buffer(consensus&, size_t max_buffered_elements);

    ss::future<append_entries_reply> enqueue(append_entries_request&& r);

    void start();
    ss::future<> stop();

private:
    using request_t = std::vector<append_entries_request>;
    using response_t = std::vector<ss::promise<append_entries_reply>>;
    using reply_t = std::variant<append_entries_reply, std::exception_ptr>;

    ss::future<> flush();
    ss::future<> do_flush(request_t, response_t);

    void propagate_results(std::vector<reply_t>&, response_t);

    consensus& _consensus;
    request_t _requests;
    response_t _responsens;
    ss::condition_variable _enequeued;
    ss::gate _gate;
    const size_t _max_buffered;
};

} // namespace raft
