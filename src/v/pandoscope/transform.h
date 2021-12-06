#pragma once

#include "model/record.h"
#include "storage/log.h"

#include <memory>
#include <vector>

namespace pandoscope {

class transformation {
public:
    struct impl {
        virtual ss::future<model::record_batch> apply(model::record_batch&) = 0;

        virtual bool is_applicable(const model::record_batch&) = 0;
        virtual ~impl() = default;
    };

    ss::future<model::record_batch> apply(model::record_batch& batch) {
        return _impl->apply(batch);
    }

    bool is_applicable(const model::record_batch& b) {
        return _impl->is_applicable(b);
    }

    explicit transformation(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

private:
    std::unique_ptr<impl> _impl;
};

class printer {
public:
    struct impl {
        virtual ss::future<> print(const model::record_batch&) = 0;

        virtual bool is_applicable(const model::record_batch&) = 0;
        virtual void summary() const = 0;

        virtual ~impl() = default;
    };

    ss::future<> print(const model::record_batch& batch) {
        return _impl->print(batch);
    }

    bool is_applicable(const model::record_batch& b) {
        return _impl->is_applicable(b);
    }

    void summary() const { _impl->summary(); }

    explicit printer(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

private:
    std::unique_ptr<impl> _impl;
};

ss::future<> transform(storage::log, storage::log, std::vector<transformation>);

ss::future<> print(storage::log, printer);

transformation make_topic_configuration_transformation();

printer make_simple_printer();

} // namespace pandoscope
