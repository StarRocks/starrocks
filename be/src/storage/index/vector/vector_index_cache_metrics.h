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
#include <cstddef>
#include <cstdint>
#include <mutex>

#include "base/metrics.h"

namespace starrocks {

class VectorIndexCacheMetrics {
public:
    VectorIndexCacheMetrics() = default;
    explicit VectorIndexCacheMetrics(MetricRegistry* registry) { install(registry); }
    ~VectorIndexCacheMetrics();

    static VectorIndexCacheMetrics* instance();

    bool install(MetricRegistry* registry);
    void hide();
    void update(size_t capacity, size_t usage, uint64_t lookup_count, uint64_t hit_count);
    void refresh();

    METRIC_DEFINE_INT_GAUGE(vector_index_cache_capacity, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_usage, MetricUnit::BYTES);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_usage_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_hit_ratio, MetricUnit::PERCENT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_dynamic_lookup_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(vector_index_cache_dynamic_hit_count, MetricUnit::NOUNIT);
    METRIC_DEFINE_DOUBLE_GAUGE(vector_index_cache_dynamic_hit_ratio, MetricUnit::PERCENT);

private:
    static uint64_t _delta_since_last_refresh(uint64_t current, uint64_t previous);

    MetricRegistry* _registry = nullptr;
    std::atomic<size_t> _capacity{0};
    std::atomic<size_t> _usage{0};
    std::atomic<uint64_t> _lookup_count{0};
    std::atomic<uint64_t> _hit_count{0};

    std::mutex _refresh_mutex;
    uint64_t _previous_lookup_count = 0;
    uint64_t _previous_hit_count = 0;
};

} // namespace starrocks
