/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/transform_utils.h"

#include "iceberg/bucket_transform_hashing_visitor.h"
#include "iceberg/time_transform_visitor.h"
#include "iceberg/transform.h"

#include <seastar/util/variant_utils.hh>

namespace iceberg {

struct transform_applying_visitor {
    explicit transform_applying_visitor(const value& source_val)
      : source_val_(source_val) {}
    const value& source_val_;

    value operator()(const identity_transform&) {
        auto primitive = std::get_if<primitive_value>(&source_val_);
        if (primitive) {
            return make_copy(*primitive);
        }
        throw std::invalid_argument(
          fmt::format("value {} must be primitive", source_val_));
    }

    value operator()(const hour_transform&) {
        int_value v{std::visit(hour_transform_visitor{}, source_val_)};
        return v;
    }

    value operator()(const day_transform&) {
        int_value v{
          std::visit(time_transform_visitor<std::chrono::days>{}, source_val_)};
        return v;
    }
    value operator()(const month_transform&) {
        int_value v{std::visit(
          time_transform_visitor<std::chrono::months>{}, source_val_)};
        return v;
    }
    value operator()(const year_transform&) {
        int_value v{std::visit(
          time_transform_visitor<std::chrono::years>{}, source_val_)};
        return v;
    }
    value operator()(const bucket_transform& tr) {
        auto primitive = std::get_if<primitive_value>(&source_val_);
        if (!primitive) {
            throw std::invalid_argument(
              fmt::format("value {} must be primitive", source_val_));
        }
        auto hash_raw = std::visit(
          bucket_transform_hashing_visitor{}, *primitive);
        auto hash
          = (static_cast<int32_t>(hash_raw) & std::numeric_limits<int32_t>::max());
        fmt::print(
          "primitive: {}, hraw: {}, h: {}\n", *primitive, hash_raw, hash);
        return int_value{hash % static_cast<int32_t>(tr.n)};
    }
    value operator()(const truncate_transform&) {
        return make_copy(source_val_);
    }
    value operator()(const void_transform&) {
        throw std::runtime_error("not implemented");
    }
};

value apply_transform(const value& source_val, const transform& transform) {
    return std::visit(transform_applying_visitor{source_val}, transform);
}

namespace {

bool is_date_or_timestamp(const primitive_type& field_type) {
    return std::holds_alternative<date_type>(field_type)
           || std::holds_alternative<timestamptz_type>(field_type)
           || std::holds_alternative<timestamptz_type>(field_type);
}

struct transform_application_validating_visitor {
    explicit transform_application_validating_visitor(
      const primitive_type& field_type)
      : field_type(field_type) {}

    std::optional<partition_spec_field_error>
    operator()(const identity_transform&) {
        return std::nullopt;
    }

    std::optional<partition_spec_field_error>
    operator()(const bucket_transform&) {
        return ss::visit(
          field_type,
          [](const int_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const long_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const date_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const time_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const timestamp_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const timestamptz_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const string_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const binary_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const fixed_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const decimal_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const uuid_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const auto& f) {
              return std::make_optional<partition_spec_field_error>(fmt::format(
                "Can not apply truncate transform to the filed {}", f));
          });
    }
    std::optional<partition_spec_field_error>
    operator()(const truncate_transform&) {
        return ss::visit(
          field_type,
          [this](const auto&) {
              return std::make_optional<partition_spec_field_error>(fmt::format(
                "Can not apply truncate transform to the filed {}",
                field_type));
          },
          [](const int_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const long_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const string_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const binary_type&) {
              return std::optional<partition_spec_field_error>{};
          },
          [](const decimal_type&) {
              return std::optional<partition_spec_field_error>{};
          });
    }
    std::optional<partition_spec_field_error>
    operator()(const year_transform& tr) {
        return validate_date_time_transform(tr);
    }
    std::optional<partition_spec_field_error>
    operator()(const month_transform& tr) {
        return validate_date_time_transform(tr);
    }
    std::optional<partition_spec_field_error>
    operator()(const day_transform& tr) {
        return validate_date_time_transform(tr);
    }
    std::optional<partition_spec_field_error>
    operator()(const hour_transform&) {
        if (
          std::holds_alternative<timestamp_type>(field_type)
          || std::holds_alternative<timestamptz_type>(field_type)) {
            return std::nullopt;
        }
        return partition_spec_field_error(fmt::format(
          "Can not apply hour transform to non timestamp field: {}",
          field_type));
    }
    std::optional<partition_spec_field_error>
    operator()(const void_transform&) {
        return std::nullopt;
    }

private:
    template<typename T>
    std::optional<partition_spec_field_error>
    validate_date_time_transform(const T& transform_type) {
        if (is_date_or_timestamp(field_type)) {
            return std::nullopt;
        }
        return partition_spec_field_error(fmt::format(
          "Can not apply {} transform to non or date timestamp field: {}",
          transform_type,
          field_type));
    }
    const primitive_type& field_type;
};
} // namespace

std::optional<partition_spec_field_error>
can_transform(const transform& tr, const field_type& field_type) {
    if (!std::holds_alternative<primitive_type>(field_type)) {
        return partition_spec_field_error(fmt::format(
          "Can not apply transform to non-primitive field: {}", field_type));
    }

    return std::visit(
      transform_application_validating_visitor(
        std::get<primitive_type>(field_type)),
      tr);
}
} // namespace iceberg
