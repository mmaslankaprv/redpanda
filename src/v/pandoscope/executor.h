#pragma once
#include "model/metadata.h"
#include "pandoscope/configuration.h"
#include "pandoscope/topics.h"
#include "pandoscope/types.h"
#include "storage/api.h"
#include "storage/fwd.h"

#include <seastar/util/log.hh>

#include <memory>
#include <vector>

namespace pandoscope {

class executor {
public:
    struct ctx {
        storage::api& storage;
        topics_state& topics;
        std::vector<std::unique_ptr<storage::kvstore>>& kvs;
    };
    class command final {
    public:
        struct impl {
            virtual ss::future<> execute(storage::log, ctx) = 0;
            virtual std::string_view name() const = 0;
            virtual ~impl() = default;
        };

        explicit command(ss::shared_ptr<impl> impl)
          : _impl(std::move(impl)) {}

        ss::future<> execute(storage::log log, ctx ctx) {
            return _impl->execute(log, ctx);
        }

        std::string_view name() const { return _impl->name(); };

    private:
        ss::shared_ptr<impl> _impl;
    };

    explicit executor(configuration);

    ss::future<> start();
    ss::future<> stop();

    ss::future<> execute(command);

    template<typename T, typename... Args>
    static command make_command(Args&&... args) {
        return command(ss::make_shared<T>(std::forward<Args>(args)...));
    }

private:
    ctx get_ctx() {
        return ctx{.storage = *_storage, .topics = _topics, .kvs = _kvs};
    }

    ss::future<> do_execute(const ntp_revision&, command);
    ss::future<std::vector<ntp_revision>> build_ntp_list();
    std::vector<ntp_revision> filter_ntps(std::vector<ntp_revision> ntps);
    ss::future<storage::log> get_log(ntp_revision);
    ss::future<std::optional<storage::log>> get_raft0_log();

    std::unique_ptr<storage::api> _storage;
    std::vector<ntp_revision> _ntps;
    configuration _cfg;
    topics_state _topics;
    std::vector<std::unique_ptr<storage::kvstore>> _kvs;
};
} // namespace pandoscope
