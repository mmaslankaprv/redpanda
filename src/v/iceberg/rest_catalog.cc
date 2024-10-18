/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/catalog.h"
#include "iceberg/rest_client/catalog_client.h"
namespace iceberg {

namespace {

template<typename T>
checked<T, iceberg::catalog::errc>
unwrap(iceberg::rest_client::expected<T> e, std::string_view context) {
    if (e.has_value()) {
        return std::move(e.value());
    }
    return map_error(std::move(e.error()), context);
}
using http_status = enum boost::beast::http::status;
using errc = enum iceberg::catalog::errc;
using enum errc;
struct domain_error_mapping_visitor {
    errc map_http_status(http_status status) const {
        if (status == http_status::not_found) {
            return not_found;
        }

        if (status == http_status::conflict) {
            return already_exists;
        }
        return unexpected_state;
    }

    errc operator()(const rest_client::http_call_error& err) const {
        return ss::visit(
          err,
          [this](http_status status) { return map_http_status(status); },
          [](const ss::sstring&) { return unexpected_state; });
    }

    errc operator()(const rest_client::json_parse_error&) const {
        return unexpected_state;
    }

    errc operator()(const rest_client::retries_exhausted&) const {
        return timedout;
    }

    errc operator()(const http::url_build_error&) const {
        return unexpected_state;
    }
};

} // namespace

// Translates domain error to more general catalog::errc.
errc map_error(
  std::string_view context, const rest_client::domain_error& error) {
    vlog(log.warn, "error returned when executing {} - {}", context, error);
    return std::visit(domain_error_mapping_visitor{}, error);
}
}; // namespace iceberg
