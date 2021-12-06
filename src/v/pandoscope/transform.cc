#include "pandoscope/transform.h"

#include "cluster/commands.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "reflection/adl.h"
#include "storage/parser_utils.h"
#include "storage/types.h"
#include "vlog.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>

#include <memory>
#include <utility>
#include <vector>

namespace pandoscope {

static ss::logger logger{"pandoscope-transform"};

model::record_batch_reader make_transforming_reader(
  std::vector<transformation> transformations,
  model::record_batch_reader&& source) {
    class transforming_reader final : public model::record_batch_reader::impl {
    private:
        using storage_t = model::record_batch_reader::storage_t;
        using data_t = model::record_batch_reader::data_t;
        using foreign_t = model::record_batch_reader::foreign_data_t;

    public:
        explicit transforming_reader(
          std::unique_ptr<model::record_batch_reader::impl> src,
          std::vector<transformation> transformations)
          : _ptr(std::move(src))
          , _transformations(std::move(transformations)) {}

        transforming_reader(const transforming_reader&) = delete;
        transforming_reader& operator=(const transforming_reader&) = delete;
        transforming_reader(transforming_reader&&) = delete;
        transforming_reader& operator=(transforming_reader&&) = delete;
        ~transforming_reader() override = default;

        bool is_end_of_stream() const final {
            // ok to copy a bool
            return _ptr->is_end_of_stream();
        }

        void print(std::ostream& os) final { _ptr->print(os); }

        data_t& get_batches(storage_t& st) {
            if (std::holds_alternative<data_t>(st)) {
                return std::get<data_t>(st);
            } else {
                return *std::get<foreign_t>(st).buffer;
            }
        }

        ss::future<storage_t>
        do_load_slice(model::timeout_clock::time_point t) final {
            return _ptr->do_load_slice(t).then(
              [this](storage_t slice) -> ss::future<storage_t> {
                  for (auto& batch : get_batches(slice)) {
                      co_await transform(batch);
                  }
                  co_return slice;
              });
        }

        ss::future<> transform(model::record_batch& b) {
            for (auto& t : _transformations) {
                if (t.is_applicable(b)) {
                    // apply transformation
                    b = co_await t.apply(b);
                }
            }
        }

    private:
        std::unique_ptr<model::record_batch_reader::impl> _ptr;
        std::vector<transformation> _transformations;
    };

    auto reader = std::make_unique<transforming_reader>(
      std::move(source).release(), std::move(transformations));

    return model::record_batch_reader(std::move(reader));
}

ss::future<> transform(
  storage::log src,
  storage::log dst,
  std::vector<transformation> transformations) {
    auto offsets = src.offsets();
    storage::log_reader_config cfg(
      offsets.start_offset, offsets.dirty_offset, ss::default_priority_class());

    auto src_rdr = co_await src.make_reader(cfg);
    auto t_rdr = make_transforming_reader(
      std::move(transformations), std::move(src_rdr));
    storage::log_append_config ap_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
    };
    auto appender = dst.make_appender(ap_cfg);
    co_await t_rdr.for_each_ref(std::move(appender), model::no_timeout);
}

struct old_topic_properties {
    std::optional<model::compression> compression;
    std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
    std::optional<model::compaction_strategy> compaction_strategy;
    std::optional<model::timestamp_type> timestamp_type;
    std::optional<size_t> segment_size;
    tristate<size_t> retention_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_duration{std::nullopt};
};

struct old_topic_configuration {
    model::topic_namespace tp_ns;
    // using signed integer because Kafka protocol defines it as signed int
    int32_t partition_count;
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor;

    old_topic_properties properties;
};

struct old_topic_configuration_assignment {
    old_topic_configuration cfg;
    std::vector<cluster::partition_assignment> assignments;
};
static constexpr int64_t zero_vint_size = vint::vint_size(0);

struct serialized_record {
    serialized_record(
      std::optional<iobuf> k,
      std::optional<iobuf> v,
      std::vector<model::record_header> hdrs
      = std::vector<model::record_header>())
      : headers(std::move(hdrs)) {
        if (k) {
            key = std::move(*k);
            encoded_key_size = key.size_bytes();
        } else {
            encoded_key_size = -1;
        }
        if (likely(v)) {
            value = std::move(*v);
            encoded_value_size = value.size_bytes();
        } else {
            encoded_value_size = -1;
        }
    }

    iobuf key;
    int32_t encoded_key_size;
    iobuf value;
    int32_t encoded_value_size;
    std::vector<model::record_header> headers;
};

uint32_t record_size(int32_t offset_delta, const serialized_record& r) {
    uint32_t size = sizeof(model::record_attributes::type)  // attributes
                    + zero_vint_size                        // timestamp delta
                    + vint::vint_size(offset_delta)         // offset_delta
                    + vint::vint_size(r.encoded_key_size)   // key size
                    + r.key.size_bytes()                    // key
                    + vint::vint_size(r.encoded_value_size) // value size
                    + r.value.size_bytes()                  // value
                    + vint::vint_size(r.headers.size());    // headers size
    for (const auto& h : r.headers) {
        size += vint::vint_size(h.key_size()) + h.key().size_bytes()
                + vint::vint_size(h.value_size()) + h.value().size_bytes();
    }
    return size;
}

model::record_batch replace_batch(
  model::record_batch_header header, std::vector<serialized_record>& records) {
    model::record_batch_header new_header = header;
    new_header.ctx.term = header.ctx.term;
    new_header.record_count = records.size();

    iobuf records_buf;
    int32_t offset_delta = 0;
    for (auto& sr : records) {
        auto rec_sz = record_size(offset_delta, sr);
        auto kz = sr.encoded_key_size;
        auto vz = sr.encoded_value_size;
        auto r = model::record(
          rec_sz,
          model::record_attributes{},
          0,
          offset_delta,
          kz,
          std::move(sr.key),
          vz,
          std::move(sr.value),
          std::move(sr.headers));
        ++offset_delta;
        model::append_record_to_buffer(records_buf, r);
    }

    storage::internal::reset_size_checksum_metadata(new_header, records_buf);
    return model::record_batch(
      new_header, std::move(records_buf), model::record_batch::tag_ctor_ng{});
}

transformation make_topic_configuration_transformation() {
    struct tr : transformation::impl {
        ss::future<model::record_batch> apply(model::record_batch& b) final {
            vlog(
              logger.info,
              "transforming topic configuration at {} - {}",
              b.base_offset());

            auto records = b.copy_records();
            iobuf_parser v_parser(records.begin()->release_value());
            iobuf_parser k_parser(records.begin()->release_key());
            // chose deserializer
            auto cmd_type = reflection::adl<cluster::command_type>{}.from(
              v_parser);
            if (cmd_type == cluster::create_topic_cmd_type) {
                auto cfg_a
                  = reflection::adl<cluster::topic_configuration_assignment>{}
                      .from(v_parser);

                vlog(
                  logger.info,
                  "transforming topic configuration at {} - {}",
                  b.base_offset(),
                  cfg_a.cfg);

                old_topic_configuration old_cfg{
                  .tp_ns = cfg_a.cfg.tp_ns,
                  .partition_count = cfg_a.cfg.partition_count,
                  .replication_factor = cfg_a.cfg.replication_factor,
                };
                old_cfg.properties.compression
                  = cfg_a.cfg.properties.compression;

                old_cfg.properties.cleanup_policy_bitflags
                  = cfg_a.cfg.properties.cleanup_policy_bitflags;
                old_cfg.properties.compaction_strategy
                  = cfg_a.cfg.properties.compaction_strategy;
                old_cfg.properties.timestamp_type
                  = cfg_a.cfg.properties.timestamp_type;
                old_cfg.properties.segment_size
                  = cfg_a.cfg.properties.segment_size;
                old_cfg.properties.retention_bytes
                  = cfg_a.cfg.properties.retention_bytes;
                old_cfg.properties.retention_duration
                  = cfg_a.cfg.properties.retention_duration;

                old_topic_configuration_assignment old_tp_as{
                  .cfg = std::move(old_cfg),
                  .assignments = std::move(cfg_a.assignments),
                };

                iobuf value_buf;
                reflection::serialize(
                  value_buf, cmd_type, std::move(old_tp_as));
                auto k = cfg_a.cfg.tp_ns;
                std::vector<serialized_record> records;

                records.push_back(serialized_record(
                  reflection::to_iobuf<model::topic_namespace>(std::move(k)),
                  std::move(value_buf)));

                co_return replace_batch(b.header(), records);
            }
            co_return b.copy();
        }

        bool is_applicable(const model::record_batch& b) final {
            return b.header().type
                   == model::record_batch_type::topic_management_cmd;
        };
    };
    return transformation(std::make_unique<tr>());
}

printer make_simple_printer() {
    struct pr : printer::impl {
        ss::future<> print(const model::record_batch& b) final {
            vlog(
              logger.info,
              "batch - offset: {}, size: {}",
              b.base_offset(),
              b.size_bytes());
            max_size = std::max<size_t>(max_size, b.size_bytes());
            co_return;
        }

        bool is_applicable(const model::record_batch&) final { return true; };

        void summary() const final {
            vlog(logger.info, "max batch size: {}", max_size);
        }

        size_t max_size = 0;
    };
    return printer(std::make_unique<pr>());
}

ss::future<> print(storage::log src, printer pr) {
    auto offsets = src.offsets();
    storage::log_reader_config cfg(
      offsets.start_offset, offsets.dirty_offset, ss::default_priority_class());
    class consumer {
    public:
        explicit consumer(printer p)
          : _printer(std::move(p)) {}

        ss::future<ss::stop_iteration>
        operator()(const model::record_batch& rb) {
            co_await _printer.print(rb);
            co_return ss::stop_iteration::no;
        }
        void end_of_stream() { _printer.summary(); }

    private:
        printer _printer;
    };

    auto rdr = co_await src.make_reader(cfg);

    co_await rdr.for_each_ref(consumer(std::move(pr)), model::no_timeout);
}

} // namespace pandoscope
