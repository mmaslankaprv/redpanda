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
#include "config/property.h"
#include "raft/logger.h"
#include "seastarx.h"

#include <seastar/core/memory.hh>
#include <seastar/core/semaphore.hh>

namespace raft {
/**
 * Thread local memory quota for raft recovery
 */
class recovery_memory_quota {
public:
    explicit recovery_memory_quota(
      config::binding<std::optional<size_t>>&& max_recovery_memory,
      config::binding<size_t>&& default_read_buffer_size)
      : _max_recovery_memory(std::move(max_recovery_memory))
      , _default_read_buffer_size(std::move(default_read_buffer_size))
      , _current_max_recovery_mem(
          _max_recovery_memory().value_or(deafault_max_memory()))
      , _memory(_current_max_recovery_mem) {
        _max_recovery_memory.watch([this] { on_max_memory_changed(); });
    }

    ss::future<ss::semaphore_units<>> acquire_read_memory() {
        return ss::get_units(
          _memory,
          std::min(_current_max_recovery_mem, _default_read_buffer_size()));
    }

private:
    static size_t deafault_max_memory() {
        return 0.15 * ss::memory::stats().total_memory();
    }
    void on_max_memory_changed() {
        int64_t new_size = _max_recovery_memory().value_or(
          deafault_max_memory());

        vlog(raftlog.info, "max recovery memory changed to {} bytes", new_size);

        int64_t diff = new_size
                       - static_cast<int64_t>(_current_max_recovery_mem);
        if (diff < 0) {
            _memory.consume(-diff);
        } else if (diff > 0) {
            _memory.signal(diff);
        }
        _current_max_recovery_mem = new_size;
    }

    config::binding<std::optional<size_t>> _max_recovery_memory;
    config::binding<size_t> _default_read_buffer_size;
    size_t _current_max_recovery_mem;
    ss::semaphore _memory;
};

} // namespace raft
