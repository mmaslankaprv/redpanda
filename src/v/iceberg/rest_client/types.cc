/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/types.h"

#include "iceberg/logger.h"

namespace iceberg::rest_client {

namespace {
struct domain_error_printing_visitor {
    std::ostream* os;

    void operator()(const http::url_build_error& err) const {
        fmt::print(*os, "url_build_error: {}", err);
    }

    void operator()(const json_parse_error& err) const {
        fmt::print(
          *os,
          "json_parse_error: context: {}, error: {}",
          err.context,
          err.error);
    }

    void operator()(const http_call_error& err) const {
        std::visit(
          [this](const auto& http_call_error) {
              fmt::print(*os, "http_call_error: {}", http_call_error);
          },
          err);
    }

    void operator()(const retries_exhausted& err) const {
        fmt::print(*os, "retries_exhausted:[{}]", fmt::join(err.errors, ", "));
    }
};
} // namespace

std::ostream& operator<<(std::ostream& os, domain_error err) {
    std::visit(domain_error_printing_visitor{.os = &os}, err);
    return os;
}

} // namespace iceberg::rest_client
