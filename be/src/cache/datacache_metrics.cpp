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

#include "cache/data_cache_hit_rate_counter.hpp"
#include "cache/datacache.h"

#ifdef WITH_STARCACHE
#include "cache/disk_cache/starcache_engine.h"
#endif

#ifdef USE_STAROS
#include "fslib/star_cache_handler.h"
#endif

namespace starrocks {

namespace {

#ifdef WITH_STARCACHE
const char* const kUpdateDataCacheMetricsHookName = "update_datacache_metrics";
#endif

} // namespace

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
    registry->register_metric("datacache_block_cache_write_bytes", &datacache_block_cache_write_bytes);
    registry->register_metric("datacache_block_cache_evict_bytes", &datacache_block_cache_evict_bytes);
}

void DataCacheMetrics::enable_update_hook(bool use_same_instance) {
    _use_same_instance.store(use_same_instance, std::memory_order_relaxed);
#ifdef WITH_STARCACHE
    if (_registry != nullptr) {
        _registry->register_hook(kUpdateDataCacheMetricsHookName, [] { DataCacheMetrics::instance()->update(); });
    }
#endif
}

void DataCacheMetrics::update() {
#ifdef WITH_STARCACHE
    auto* cache_env = DataCache::GetInstance();
    const auto* local_mem_cache = cache_env->local_mem_cache();
    DataCacheMemMetrics mem_metrics{};
    if (local_mem_cache && local_mem_cache->is_initialized()) {
        mem_metrics = local_mem_cache->cache_metrics();
    }
    auto* local_disk_cache = cache_env->local_disk_cache();
    DataCacheDiskMetrics disk_metrics{};
    int64_t meta_used_bytes = 0;
    if (local_disk_cache && local_disk_cache->is_initialized()) {
        disk_metrics = local_disk_cache->cache_metrics();
        auto* starcache = static_cast<StarCacheEngine*>(local_disk_cache);
        auto&& star_metrics = starcache->starcache_metrics(0);
        meta_used_bytes = star_metrics.meta_used_bytes;
    }
#ifdef USE_STAROS
    if (!_use_same_instance.load(std::memory_order_relaxed)) {
        starcache::CacheMetrics starlet_cache_metrics{};
        staros::starlet::fslib::star_cache_get_metrics(&starlet_cache_metrics);
        // merge the disk cache metrics
        disk_metrics.disk_quota_bytes += starlet_cache_metrics.disk_quota_bytes;
        disk_metrics.disk_used_bytes += starlet_cache_metrics.disk_used_bytes;
        meta_used_bytes += starlet_cache_metrics.mem_used_bytes;
    }
#endif
    datacache_mem_quota_bytes.set_value(mem_metrics.mem_quota_bytes);
    datacache_mem_used_bytes.set_value(mem_metrics.mem_used_bytes);
    datacache_disk_quota_bytes.set_value(disk_metrics.disk_quota_bytes);
    datacache_disk_used_bytes.set_value(disk_metrics.disk_used_bytes);
    datacache_meta_used_bytes.set_value(meta_used_bytes);

    // Starcache detail_l2 carries cumulative populate (write_bytes) and eviction (remove_bytes)
    // totals. Sample them so dashboards can derive eviction pressure (evict-rate vs write-rate)
    // without scraping the HTTP /api/datacache/stat endpoint.
    if (local_disk_cache != nullptr && local_disk_cache->is_initialized()) {
        auto* starcache = static_cast<StarCacheEngine*>(local_disk_cache);
        auto detail_metrics = starcache->starcache_metrics(2);
        if (detail_metrics.detail_l2 != nullptr) {
            datacache_block_cache_write_bytes.set_value(detail_metrics.detail_l2->write_bytes);
            datacache_block_cache_evict_bytes.set_value(detail_metrics.detail_l2->remove_bytes);
        }
    }

    // Update hit rate metrics from DataCacheHitRateCounter
    auto* hit_rate_counter = DataCacheHitRateCounter::instance();
    block_cache_hit_bytes.set_value(hit_rate_counter->block_cache_hit_bytes());
    block_cache_miss_bytes.set_value(hit_rate_counter->block_cache_miss_bytes());
#endif
}

} // namespace starrocks
