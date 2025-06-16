// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>

namespace starrocks::pipeline {
using std::chrono::milliseconds;
using std::chrono::steady_clock;
class TopnRfBackPressure {
    enum Phase { PH_UNTHROTTLE, PH_THROTTLE, PH_PASS_THROUGH };

    template <typename T>
    class ScaleGenerator {
    public:
        ScaleGenerator(T initial_value, T delta, double factor, std::function<bool(T)> next_cb)
                : initial_value(initial_value), delta(delta), factor(factor), next_cb(next_cb), value(initial_value) {}

        T limit() { return value; }
        void next() {
            value += delta;
            value *= factor;
        }
        bool has_next() { return next_cb(value); }

    private:
        const T initial_value;
        const T delta;
        const double factor;
        const std::function<bool(T)> next_cb;
        T value;
    };

public:
    void update_selectivity(double selectivity) { _current_selectivity = selectivity; }
    void inc_num_rows(size_t num_rows) { _current_num_rows += num_rows; }

    bool should_throttle() {
        if (_phase == PH_PASS_THROUGH) {
            return false;
        } else if (!_round_limiter.has_next() || !_throttle_time_limiter.has_next() || !_num_rows_limiter.has_next() ||
                   _current_selectivity <= _selectivity_lower_bound ||
                   _current_total_throttle_time >= _throttle_time_upper_bound) {
            _phase = PH_PASS_THROUGH;
            return false;
        }

        if (_phase == PH_UNTHROTTLE) {
            if (_current_num_rows <= _num_rows_limiter.limit()) {
                return false;
            }
            _phase = PH_THROTTLE;
            _current_throttle_deadline = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count() +
                                         _throttle_time_limiter.limit();
            return true;
        } else {
            auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
            if (now < _current_throttle_deadline) {
                return true;
            }
            _phase = PH_UNTHROTTLE;
            _current_num_rows = 0;
            _current_total_throttle_time += _throttle_time_limiter.limit();
            _round_limiter.next();
            _throttle_time_limiter.next();
            _num_rows_limiter.next();
            return false;
        }
    }

    TopnRfBackPressure(double selectivity_lower_bound, int64_t throttle_time_upper_bound, int max_rounds,
                       int64_t throttle_time, size_t num_rows)
            : _selectivity_lower_bound(selectivity_lower_bound),
              _throttle_time_upper_bound(throttle_time_upper_bound),
              _round_limiter(0, 1, 1.0, [max_rounds](int r) { return r < max_rounds; }),
              _throttle_time_limiter(throttle_time, 0, 1.0, [](int64_t) { return true; }),
              _num_rows_limiter(num_rows, 0, 2.0, [](size_t n) { return n < std::numeric_limits<size_t>::max() / 2; }) {
    }

private:
    const double _selectivity_lower_bound;
    const int64_t _throttle_time_upper_bound;
    Phase _phase{PH_UNTHROTTLE};
    ScaleGenerator<int> _round_limiter;
    ScaleGenerator<int64_t> _throttle_time_limiter;
    ScaleGenerator<int64_t> _num_rows_limiter;
    int64_t _current_throttle_deadline{-1};
    int64_t _current_total_throttle_time{0};
    size_t _current_num_rows{0};
    double _current_selectivity{1.0};
};

} // namespace starrocks::pipeline
