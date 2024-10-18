/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/catalog_client.h"

#include <seastar/core/sleep.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;

class client_mock : public http::abstract_client {
public:
    MOCK_METHOD(
      ss::future<http::downloaded_response>,
      request_and_collect_response,
      (boost::beast::http::request_header<>&&,
       std::optional<iobuf>,
       ss::lowres_clock::duration),
      (override));

    MOCK_METHOD(ss::future<>, shutdown_and_stop, (), (override));
};

static constexpr auto endpoint = "http://localhost:8081";
struct RestCatalogTest : Test {
    struct client_source : public iceberg::rest_client::client_source {
        explicit client_source(
          std::function<void(std::reference_wrapper<client_mock>)> expectation)
          : _expectation{std::move(expectation)} {}

        std::unique_ptr<http::abstract_client> acquire() final {
            auto ptr = std::make_unique<client_mock>();
            _expectation(*(ptr.get()));

            // Client passed to caller should always be shut down, and exactly
            // once
            EXPECT_CALL(*(ptr.get()), shutdown_and_stop())
              .Times(1)
              .WillOnce(Return(ss::now()));
            return ptr;
        }

    private:
        std::function<void(std::reference_wrapper<client_mock>)> _expectation;
    };

    iceberg::rest_client::catalog_client
    make_catalog_client(client_source& src) {
        return {
          src,
          endpoint,
          creds,
          iceberg::rest_client::base_path{"/catalog"},
          std::nullopt,
          iceberg::rest_client::api_version("v1")};
    }

    iceberg::rest_client::credentials creds{
      .client_id = "redpanda",
      .client_secret = "being cute",
    };
};

TEST_F(RestCatalogTest, CheckTokenIsAcquiredOnlyOnce) {}
