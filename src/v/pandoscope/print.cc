#include "pandoscope/print.h"

#include "pandoscope/logger.h"
#include "pandoscope/utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace pandoscope {

ss::future<> print(storage::log log) {
    class consumer {
    public:
        ss::future<ss::stop_iteration>
        operator()(const model::record_batch& rb) {
            vlog(
              logger.info,
              "batch - offset: {}, size: {}",
              rb.base_offset(),
              rb.size_bytes());
            co_return ss::stop_iteration::no;
        }
        void end_of_stream() {}
    };

    auto rdr = co_await read_all_reader(log);

    co_await rdr.for_each_ref(consumer{}, model::no_timeout);
}

executor::command print_command() {
    struct cmd : executor::command::impl {
        ss::future<> execute(storage::log log, executor::ctx) final {
            return print(log);
        }

        std::string_view name() const final { return "printer"; };
    };

    return executor::command(ss::make_shared<cmd>());
}

} // namespace pandoscope
