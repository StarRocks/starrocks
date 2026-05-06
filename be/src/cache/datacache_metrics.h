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

#include "base/metrics.h"

namespace starrocks {

// Data Cache process-level metrics for memory, disk, metadata, and app-observed
// block-cache hit bytes.
class DataCacheMetrics {
public:
    DataCacheMetrics() = default;
    explicit DataCacheMetrics(MetricRegistry* registry) { install(registry); }
    ~DataCacheMetrics() = default;

    static DataCacheMetrics* instance();

    void install(MetricRegistry* registry);

    // Register the runtime update hook after startup determines whether StarOS
    // and DataCache share the same StarCache instance.
    void enable_update_hook(bool use_same_instance);
    void update();

    METRIC_DEFINE_INT_GAUGE(datacache_mem_quota_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(datacache_mem_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(datacache_disk_quota_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(datacache_disk_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_GAUGE(datacache_meta_used_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(block_cache_hit_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_ATOMIC_COUNTER(block_cache_miss_bytes, MetricUnit::BYTES);

private:
    MetricRegistry* _registry = nullptr;
    std::atomic<bool> _use_same_instance{false};
};

} // namespace starrocks
