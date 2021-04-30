/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include <array>
#include <numeric>

/*
 * simple moving average
 */

template<typename T, size_t Samples>
class moving_average {
public:
    explicit moving_average(T initial)
      : _value(initial) {
        for (auto& s : _samples) {
            s = _value;
        }
    }

    void update(T v) {
        _samples[_idx] = v;
        _idx = (++_idx % Samples);
        _value = std::accumulate(_samples.begin(), _samples.end(), T{0})
                 / Samples;
    }

    T get() const { return _value; }

private:
    std::array<T, Samples> _samples;
    T _value;
    size_t _idx{0};
};
