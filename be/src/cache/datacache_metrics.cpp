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

#include "cache/datacache.h"

#ifdef USE_STAROS
#include "fslib/star_cache_handler.h"
#endif
#include "util/starrocks_metrics.h"

namespace starrocks {

#ifndef WITH_STARCACHE
// empty implementation if WITH_STARCACHE is not enabled
void register_datacache_metrics(bool) {}
#else
static void update_datacache_metrics(bool use_same_instance) {
    auto* cache_env = DataCache::GetInstance();
    const auto* local_disk_cache = cache_env->local_disk_cache();
    DataCacheDiskMetrics disk_metrics{};
    if (local_disk_cache && local_disk_cache->is_initialized()) {
        disk_metrics = local_disk_cache->cache_metrics();
    }
    const auto* local_mem_cache = cache_env->local_mem_cache();
    DataCacheMemMetrics mem_metrics{};
    if (local_mem_cache && local_mem_cache->is_initialized()) {
        mem_metrics = local_mem_cache->cache_metrics();
    }
#ifdef USE_STAROS
    if (!use_same_instance) {
        starcache::CacheMetrics starlet_cache_metrics{};
        staros::starlet::fslib::star_cache_get_metrics(&starlet_cache_metrics);
        // merge the disk cache metrics
        disk_metrics.disk_quota_bytes += starlet_cache_metrics.disk_quota_bytes;
        disk_metrics.disk_used_bytes += starlet_cache_metrics.disk_used_bytes;
    }
#endif
    StarRocksMetrics::instance()->datacache_mem_quota_bytes.set_value(mem_metrics.mem_quota_bytes);
    StarRocksMetrics::instance()->datacache_mem_used_bytes.set_value(mem_metrics.mem_used_bytes);
    StarRocksMetrics::instance()->datacache_disk_quota_bytes.set_value(disk_metrics.disk_quota_bytes);
    StarRocksMetrics::instance()->datacache_disk_used_bytes.set_value(disk_metrics.disk_used_bytes);
}

void register_datacache_metrics(bool use_same_instance) {
    StarRocksMetrics::instance()->metrics()->register_hook(
            "update_datacache_metrics", [use_same = use_same_instance] { update_datacache_metrics(use_same); });
}
#endif

} // namespace starrocks
