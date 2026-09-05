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
class DataCacheHitRateCounter {
public:
    static DataCacheHitRateCounter* instance() {
        static DataCacheHitRateCounter counter;
        return &counter;
    }
    ~DataCacheHitRateCounter() = default;

    void update_block_cache_stat(uint64_t block_cache_hit_bytes, uint64_t block_cache_miss_bytes) {
        _block_cache_hit_bytes << block_cache_hit_bytes;
        _block_cache_miss_bytes << block_cache_miss_bytes;
    }
    void update_page_cache_stat(uint64_t page_cache_hit_count, uint64_t page_read_count) {
        _page_cache_hit_count << page_cache_hit_count;
        if (page_read_count > page_cache_hit_count) {
            _page_cache_miss_count << (page_read_count - page_cache_hit_count);
        }
    }

    ssize_t block_cache_hit_bytes() const { return _block_cache_hit_bytes.get_value(); }
    ssize_t block_cache_miss_bytes() const { return _block_cache_miss_bytes.get_value(); }
    ssize_t block_cache_hit_bytes_last_minute() const { return _block_cache_hit_bytes_last_minute.get_value(); }
    ssize_t block_cache_miss_bytes_last_minute() const { return _block_cache_miss_bytes_last_minute.get_value(); }
    double block_cache_hit_rate() const {
        return _hit_rate_calculate(_block_cache_hit_bytes.get_value(), _block_cache_miss_bytes.get_value());
    }
    double block_cache_hit_rate_last_minute() const {
        return _hit_rate_calculate(_block_cache_hit_bytes_last_minute.get_value(),
                                   _block_cache_miss_bytes_last_minute.get_value());
    }

    ssize_t page_cache_hit_count() const { return _page_cache_hit_count.get_value(); }
    ssize_t page_cache_miss_count() const { return _page_cache_miss_count.get_value(); }
    ssize_t page_cache_hit_count_last_minute() const { return _page_cache_hit_count_last_minute.get_value(); }
    ssize_t page_cache_miss_count_last_minute() const { return _page_cache_miss_count_last_minute.get_value(); }
    double page_cache_hit_rate() const {
        return _hit_rate_calculate(_page_cache_hit_count.get_value(), _page_cache_miss_count.get_value());
    }
    double page_cache_hit_rate_last_minute() const {
        return _hit_rate_calculate(_page_cache_hit_count_last_minute.get_value(),
                                   _page_cache_miss_count_last_minute.get_value());
    }

    void reset() {
        _block_cache_hit_bytes.reset();
        _block_cache_miss_bytes.reset();

        _page_cache_hit_count.reset();
        _page_cache_miss_count.reset();
    }

private:
    static double _hit_rate_calculate(ssize_t hit, ssize_t miss) {
        ssize_t total = hit + miss;
        if (total > 0) {
            double hit_rate = std::round(double(hit) / double(total) * 100.0) / 100.0;
            return hit_rate;
        } else {
            return 0;
        }
    }

    bvar::Adder<ssize_t> _block_cache_hit_bytes;
    bvar::Adder<ssize_t> _block_cache_miss_bytes;
    bvar::Window<bvar::Adder<ssize_t>> _block_cache_hit_bytes_last_minute{&_block_cache_hit_bytes, 60};
    bvar::Window<bvar::Adder<ssize_t>> _block_cache_miss_bytes_last_minute{&_block_cache_miss_bytes, 60};

    bvar::Adder<ssize_t> _page_cache_hit_count;
    bvar::Adder<ssize_t> _page_cache_miss_count;
    bvar::Window<bvar::Adder<ssize_t>> _page_cache_hit_count_last_minute{&_page_cache_hit_count, 60};
    bvar::Window<bvar::Adder<ssize_t>> _page_cache_miss_count_last_minute{&_page_cache_miss_count, 60};
};
} // namespace starrocks
