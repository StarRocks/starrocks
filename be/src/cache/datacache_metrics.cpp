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

#include "cache/datacache_metrics.h"

namespace starrocks {

DataCacheMetrics* DataCacheMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new DataCacheMetrics();
    return instance;
}

void DataCacheMetrics::install(MetricRegistry* registry) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        return;
    }
    _registry = registry;

    registry->register_metric("datacache_mem_quota_bytes", &datacache_mem_quota_bytes);
    registry->register_metric("datacache_mem_used_bytes", &datacache_mem_used_bytes);
    registry->register_metric("datacache_disk_quota_bytes", &datacache_disk_quota_bytes);
    registry->register_metric("datacache_disk_used_bytes", &datacache_disk_used_bytes);
    registry->register_metric("datacache_meta_used_bytes", &datacache_meta_used_bytes);
    registry->register_metric("block_cache_hit_bytes", &block_cache_hit_bytes);
    registry->register_metric("block_cache_miss_bytes", &block_cache_miss_bytes);
}

void DataCacheMetrics::update(const DataCacheMetricsSnapshot& snapshot) {
    datacache_mem_quota_bytes.set_value(snapshot.mem_quota_bytes);
    datacache_mem_used_bytes.set_value(snapshot.mem_used_bytes);
    datacache_disk_quota_bytes.set_value(snapshot.disk_quota_bytes);
    datacache_disk_used_bytes.set_value(snapshot.disk_used_bytes);
    datacache_meta_used_bytes.set_value(snapshot.meta_used_bytes);
    block_cache_hit_bytes.set_value(snapshot.block_cache_hit_bytes);
    block_cache_miss_bytes.set_value(snapshot.block_cache_miss_bytes);
}

} // namespace starrocks
