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

#include <bvar/bvar.h>

namespace starrocks {
class BlockCacheHitRateCounter {
public:
    static BlockCacheHitRateCounter* instance() {
        static BlockCacheHitRateCounter counter;
        return &counter;
    }
    ~BlockCacheHitRateCounter() = default;
    void update(uint64_t hit_bytes, uint64_t miss_bytes) {
        _hit_bytes << hit_bytes;
        _miss_bytes << miss_bytes;
    }
    double hit_rate() const { return hit_rate_calculate(_hit_bytes.get_value(), _miss_bytes.get_value()); }
    double hit_rate_last_minute() const {
        return hit_rate_calculate(_hit_bytes_last_minute.get_value(), _miss_bytes_last_minute.get_value());
    }
    ssize_t get_hit_bytes() const { return _hit_bytes.get_value(); }
    ssize_t get_miss_bytes() const { return _miss_bytes.get_value(); }
    ssize_t get_hit_bytes_last_minute() const { return _hit_bytes_last_minute.get_value(); }
    ssize_t get_miss_bytes_last_minute() const { return _miss_bytes_last_minute.get_value(); }
    void reset() {
        _hit_bytes.reset();
        _miss_bytes.reset();
    }

private:
    static double hit_rate_calculate(ssize_t hit_bytes, ssize_t miss_bytes) {
        ssize_t total_bytes = hit_bytes + miss_bytes;
        if (total_bytes > 0) {
            double hit_rate = std::round(double(hit_bytes) / double(total_bytes) * 100.0) / 100.0;
            return hit_rate;
        } else {
            return 0;
        }
    }
    bvar::Adder<ssize_t> _hit_bytes;
    bvar::Adder<ssize_t> _miss_bytes;
    bvar::Window<bvar::Adder<ssize_t>> _hit_bytes_last_minute{&_hit_bytes, 60};
    bvar::Window<bvar::Adder<ssize_t>> _miss_bytes_last_minute{&_miss_bytes, 60};
};
} // namespace starrocks
