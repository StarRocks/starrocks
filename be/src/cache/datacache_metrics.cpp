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

    registry->register_metric("block_cache_hit_count", &block_cache_hit_count);
    registry->register_metric("block_cache_miss_count", &block_cache_miss_count);
    registry->register_metric("block_cache_hit_count_last_minute", &block_cache_hit_count_last_minute);
    registry->register_metric("block_cache_miss_count_last_minute", &block_cache_miss_count_last_minute);
    registry->register_metric("block_cache_hit_bytes_last_minute", &block_cache_hit_bytes_last_minute);
    registry->register_metric("block_cache_miss_bytes_last_minute", &block_cache_miss_bytes_last_minute);
    registry->register_metric("block_cache_read_mem_bytes", &block_cache_read_mem_bytes);
    registry->register_metric("block_cache_read_disk_bytes", &block_cache_read_disk_bytes);
    registry->register_metric("block_cache_write_bytes", &block_cache_write_bytes);
    registry->register_metric("block_cache_write_success_count", &block_cache_write_success_count);
    registry->register_metric("block_cache_write_fail_count", &block_cache_write_fail_count);
    registry->register_metric("block_cache_remove_bytes", &block_cache_remove_bytes);
    registry->register_metric("block_cache_remove_success_count", &block_cache_remove_success_count);
    registry->register_metric("block_cache_remove_fail_count", &block_cache_remove_fail_count);
    registry->register_metric("block_cache_current_reading_count", &block_cache_current_reading_count);
    registry->register_metric("block_cache_current_writing_count", &block_cache_current_writing_count);
    registry->register_metric("block_cache_current_removing_count", &block_cache_current_removing_count);
    registry->register_metric("block_cache_buffer_item_count", &block_cache_buffer_item_count);
    registry->register_metric("block_cache_buffer_item_bytes", &block_cache_buffer_item_bytes);

    registry->register_metric("datacache_page_hit_count", &datacache_page_hit_count);
    registry->register_metric("datacache_page_miss_count", &datacache_page_miss_count);
    registry->register_metric("datacache_page_hit_count_last_minute", &datacache_page_hit_count_last_minute);
    registry->register_metric("datacache_page_miss_count_last_minute", &datacache_page_miss_count_last_minute);
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
        auto&& star_metrics = starcache->starcache_metrics(2);
        meta_used_bytes = star_metrics.meta_used_bytes;

        block_cache_hit_count.set_value(star_metrics.detail_l1->hit_count);
        block_cache_miss_count.set_value(star_metrics.detail_l1->miss_count);
        block_cache_hit_count_last_minute.set_value(star_metrics.detail_l2->hit_count_last_minite);
        block_cache_miss_count_last_minute.set_value(star_metrics.detail_l2->miss_count_last_minite);
        block_cache_hit_bytes_last_minute.set_value(star_metrics.detail_l2->hit_bytes_last_minite);
        block_cache_miss_bytes_last_minute.set_value(star_metrics.detail_l2->miss_bytes_last_minite);
        block_cache_read_mem_bytes.set_value(star_metrics.detail_l2->read_mem_bytes);
        block_cache_read_disk_bytes.set_value(star_metrics.detail_l2->read_disk_bytes);
        block_cache_write_bytes.set_value(star_metrics.detail_l2->write_bytes);
        block_cache_write_success_count.set_value(star_metrics.detail_l2->write_success_count);
        block_cache_write_fail_count.set_value(star_metrics.detail_l2->write_fail_count);
        block_cache_remove_bytes.set_value(star_metrics.detail_l2->remove_bytes);
        block_cache_remove_success_count.set_value(star_metrics.detail_l2->remove_success_count);
        block_cache_remove_fail_count.set_value(star_metrics.detail_l2->remove_fail_count);
        block_cache_current_reading_count.set_value(star_metrics.detail_l2->current_reading_count);
        block_cache_current_writing_count.set_value(star_metrics.detail_l2->current_writing_count);
        block_cache_current_removing_count.set_value(star_metrics.detail_l2->current_removing_count);
        block_cache_buffer_item_count.set_value(star_metrics.detail_l2->buffer_item_count);
        block_cache_buffer_item_bytes.set_value(star_metrics.detail_l2->buffer_item_bytes);
    }
#ifdef USE_STAROS
    if (!_use_same_instance.load(std::memory_order_relaxed)) {
        starcache::CacheMetrics starlet_cache_metrics{};
        staros::starlet::fslib::star_cache_get_metrics(&starlet_cache_metrics);
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

    auto* hit_rate_counter = DataCacheHitRateCounter::instance();
    block_cache_hit_bytes.set_value(hit_rate_counter->block_cache_hit_bytes());
    block_cache_miss_bytes.set_value(hit_rate_counter->block_cache_miss_bytes());
    datacache_page_hit_count.set_value(hit_rate_counter->page_cache_hit_count());
    datacache_page_miss_count.set_value(hit_rate_counter->page_cache_miss_count());
    datacache_page_hit_count_last_minute.set_value(hit_rate_counter->page_cache_hit_count_last_minute());
    datacache_page_miss_count_last_minute.set_value(hit_rate_counter->page_cache_miss_count_last_minute());
#endif
}

} // namespace starrocks
