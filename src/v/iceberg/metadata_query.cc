// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/metadata_query.h"

namespace iceberg {

namespace {

template<result_type ResultT>
struct results_collector {
    using result_t = internal::results_container<ResultT>::type;
    ss::stop_iteration collect(const snapshot& s) {
        if constexpr (ResultT == result_type::snapshot) {
            result_.push_back(s);
            return seastar::stop_iteration::yes;
        }
        vassert(
          false,
          "Trying to collect snapshot while result type is: {}",
          fmt::underlying(ResultT));
    }
    ss::stop_iteration
    collect(const snapshot& snap, const manifest_file& m_file) {
        if constexpr (ResultT == result_type::snapshot) {
            result_.push_back(snap);
            return seastar::stop_iteration::yes;
        } else if constexpr (ResultT == result_type::manifest_file) {
            if (!manifests_.contains(m_file.manifest_path)) {
                manifests_.emplace(m_file.manifest_path);
                result_.push_back(m_file.copy());
            }
            return seastar::stop_iteration::no;
        }
        vassert(
          false,
          "Trying to collect snapshot or manifest_file while result type is: "
          "{}",
          fmt::underlying(ResultT));
        __builtin_unreachable();
    }
    ss::stop_iteration collect(
      const snapshot& snap,
      const manifest_file& m_file,
      const manifest& manifest) {
        if constexpr (ResultT == result_type::snapshot) {
            result_.push_back(snap);
            return seastar::stop_iteration::yes;
        } else if constexpr (ResultT == result_type::manifest_file) {
            if (!manifests_.contains(m_file.manifest_path)) {
                manifests_.emplace(m_file.manifest_path);
                result_.push_back(m_file.copy());
            }
            return seastar::stop_iteration::yes;
        } else if constexpr (ResultT == result_type::manifest) {
            if (!manifests_.contains(m_file.manifest_path)) {
                manifests_.emplace(m_file.manifest_path);
                result_.push_back(manifest.copy());
            }

            return seastar::stop_iteration::no;
        }
        vassert(
          false,
          "Trying to collect snapshot, manifest_file or manifest while result "
          "type is: "
          "{}",
          fmt::underlying(ResultT));
    }

    internal::results_container<ResultT>::type release() && {
        manifests_.clear();
        return std::move(result_);
    }

private:
    // container used for deduplication based on path
    chunked_hash_set<ss::sstring> manifests_;
    internal::results_container<ResultT>::type result_;
};

template<result_type ResultT>
bool needs_manifest_list(const metadata_query<ResultT>& query) {
    return query.manifest_file_matcher.has_value()
           || query.manifest_matcher.has_value()
           || query.r_type >= result_type::manifest_file;
}
template<result_type ResultT>
bool needs_manifest(const metadata_query<ResultT>& query) {
    return query.manifest_matcher.has_value()
           || query.r_type == result_type::manifest;
}
template<result_type ResultT>
bool matches(const metadata_query<ResultT>& query, const snapshot& s) {
    return !query.snapshot_matcher.has_value() || (*query.snapshot_matcher)(s);
}
template<result_type ResultT>
bool matches(const metadata_query<ResultT>& query, const manifest_file& s) {
    return !query.manifest_file_matcher.has_value()
           || (*query.manifest_file_matcher)(s);
}
template<result_type ResultT>
bool matches(const metadata_query<ResultT>& query, const manifest& s) {
    return !query.manifest_matcher.has_value() || (*query.manifest_matcher)(s);
}

checked<partition_key_type, iceberg::query_executor::errc>
make_partition_key_type(
  const table_metadata& table,
  const snapshot& snap,
  const manifest_file& m_file) {
    auto s_it = std::find_if(
      table.schemas.begin(),
      table.schemas.end(),
      [s_id = snap.schema_id](const schema& sch) {
          return sch.schema_id == s_id;
      });

    if (s_it == table.schemas.end()) {
        return iceberg::query_executor::errc::table_metadata_inconsistency;
    }

    auto p_spec_it = std::find_if(
      table.partition_specs.begin(),
      table.partition_specs.end(),
      [id = m_file.partition_spec_id](const partition_spec& p_spec) {
          return p_spec.spec_id == id;
      });

    if (p_spec_it == table.partition_specs.end()) {
        return iceberg::query_executor::errc::table_metadata_inconsistency;
    }

    return partition_key_type::create(*p_spec_it, *s_it);
}

} // namespace

template<result_type ResultT>
ss::future<checked<
  typename internal::results_container<ResultT>::type,
  query_executor::errc>>
query_executor::do_execute_query(const metadata_query<ResultT>& query) const {
    results_collector<ResultT> collector;

    if (!table_.snapshots) {
        co_return std::move(collector).release();
    }
    const auto& snapshots = table_.snapshots.value();
    // simple DFS scan over metadata
    for (const auto& s : snapshots) {
        if (!matches(query, s)) {
            continue;
        }
        // snapshot matches, check if manifest list is needed, otherwise simply
        // collect the snapshot
        if (needs_manifest_list(query)) {
            auto m_list_result = co_await io_.download_manifest_list(
              manifest_list_path{s.manifest_list_path});

            if (m_list_result.has_error()) {
                vlog(
                  log.warn,
                  "error downloading manifest list from {} - {}",
                  s.manifest_list_path,
                  fmt::underlying_t<metadata_io::errc>(m_list_result.error()));
                co_return errc::metadata_io_error;
            }

            for (auto& manifest_file : m_list_result.value().files) {
                if (!matches(query, manifest_file)) {
                    continue;
                }

                if (needs_manifest(query)) {
                    auto pk_result = make_partition_key_type(
                      table_, s, manifest_file);
                    if (pk_result.has_error()) {
                        co_return pk_result.error();
                    }
                    auto m_result = co_await io_.download_manifest(
                      manifest_path(manifest_file.manifest_path),
                      std::move(pk_result.value()));

                    if (m_result.has_error()) {
                        vlog(
                          log.warn,
                          "error downloading manifest from {} - {}",
                          manifest_file.manifest_path,
                          fmt::underlying_t<metadata_io::errc>(
                            m_result.error()));
                        co_return errc::metadata_io_error;
                    }
                    if (matches(query, m_result.value())) {
                        if (collector.collect(
                              s, manifest_file, m_result.value())) {
                            break;
                        }
                    }
                } else {
                    if (collector.collect(s, manifest_file)) {
                        break;
                    }
                }
            }
        } else {
            collector.collect(s);
        }
    }

    co_return std::move(collector).release();
}

ss::future<checked<chunked_vector<snapshot>, query_executor::errc>>
query_executor::execute_query(const metadata_query<result_type::snapshot>& query) const {
    return do_execute_query(query);
}

ss::future<checked<chunked_vector<manifest_file>, query_executor::errc>>
query_executor::execute_query(
  const metadata_query<result_type::manifest_file>& query) const {
    return do_execute_query(query);
}

ss::future<checked<chunked_vector<manifest>, query_executor::errc>>
query_executor::execute_query(const metadata_query<result_type::manifest>& query) const {
    return do_execute_query(query);
}
} // namespace iceberg
