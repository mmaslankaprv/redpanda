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

class rest_catalog : public catalog {
public:
    explicit rest_catalog(rest_client::catalog_client);

    ss::future<checked<table_metadata, errc>> create_table(
      const table_identifier& table_ident,
      const schema& schema,
      const partition_spec& spec) final;

    ss::future<checked<table_metadata, errc>>
    load_table(const table_identifier& table_ident) final;

    ss::future<checked<std::nullopt_t, errc>>
    commit_txn(const table_identifier& table_ident, transaction) final;

private:
    rest_client::catalog_client _client;
    ss::abort_source _as;
};
}; // namespace iceberg
