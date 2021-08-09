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
#include "rpc/netbuf.h"
#include "rpc/server.h"
#include "rpc/service.h"
namespace rpc {
struct server_context_impl;
class simple_protocol final : public server::protocol {
public:
    template<typename T, typename... Args>
    void register_service(Args&&... args) {
        static_assert(std::is_base_of_v<service, T>, "must extend service.h");
        _services.push_back(std::make_unique<T>(std::forward<Args>(args)...));
    }

    const char* name() const final {
        return "vectorized internal rpc protocol";
    };
    ss::future<> apply(server::resources) final;

private:
    ss::future<> dispatch_method_once(header, server::resources);
    void dispatch_in_background(
      uint32_t, server::resources, ss::lw_shared_ptr<server_context_impl>);

    std::vector<std::unique_ptr<service>> _services;
};

} // namespace rpc
