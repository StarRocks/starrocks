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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/page_cache.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/page_cache.h"

#include <malloc.h>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/defer_op.h"
#include "util/lru_cache.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

METRIC_DEFINE_UINT_GAUGE(page_cache_lookup_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(page_cache_hit_count, MetricUnit::OPERATIONS);
METRIC_DEFINE_UINT_GAUGE(page_cache_capacity, MetricUnit::BYTES);

StoragePageCache* StoragePageCache::_s_instance = nullptr;

static void init_metrics() {
    StarRocksMetrics::instance()->metrics()->register_metric("page_cache_lookup_count", &page_cache_lookup_count);
    StarRocksMetrics::instance()->metrics()->register_hook("page_cache_lookup_count", []() {
        page_cache_lookup_count.set_value(StoragePageCache::instance()->get_lookup_count());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("page_cache_hit_count", &page_cache_hit_count);
    StarRocksMetrics::instance()->metrics()->register_hook("page_cache_hit_count", []() {
        page_cache_hit_count.set_value(StoragePageCache::instance()->get_hit_count());
    });

    StarRocksMetrics::instance()->metrics()->register_metric("page_cache_capacity", &page_cache_capacity);
    StarRocksMetrics::instance()->metrics()->register_hook("page_cache_capacity", []() {
        page_cache_capacity.set_value(StoragePageCache::instance()->get_capacity());
    });
}

void StoragePageCache::create_global_cache(ObjectCache* obj_cache) {
    if (_s_instance == nullptr) {
        _s_instance = new StoragePageCache(obj_cache);
        init_metrics();
    }
}

void StoragePageCache::release_global_cache() {
    if (_s_instance != nullptr) {
        delete _s_instance;
        _s_instance = nullptr;
    }
}

void StoragePageCache::prune() {
    _cache->prune();
}

void StoragePageCache::set_capacity(size_t capacity) {
    Status st = _cache->set_capacity(capacity);
    LOG_IF(INFO, !st.ok()) << "Fail to set cache capacity to " << capacity << ", reason: " << st.message();
}

size_t StoragePageCache::get_capacity() {
    return _cache->capacity();
}

uint64_t StoragePageCache::get_lookup_count() {
    return _cache->lookup_count();
}

uint64_t StoragePageCache::get_hit_count() {
    return _cache->hit_count();
}

bool StoragePageCache::adjust_capacity(int64_t delta, size_t min_capacity) {
    Status st = _cache->adjust_capacity(delta, min_capacity);
    if (!st.ok()) {
        LOG_IF(INFO, !st.ok()) << "Fail to adjust cache capacity, delta: " << delta << ", reason: " << st.message();
        return false;
    }
    return true;
}

bool StoragePageCache::lookup(const CacheKey& key, PageCacheHandle* handle) {
    ObjectCacheHandle* obj_handle = nullptr;
    Status st = _cache->lookup(key.encode(), &obj_handle);
    if (!st.ok()) {
        return false;
    }
    *handle = PageCacheHandle(_cache, obj_handle);
    return true;
}

Status StoragePageCache::insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle, bool in_memory) {
#ifndef BE_TEST
    int64_t mem_size = malloc_usable_size(data.data);
    tls_thread_status.mem_release(mem_size);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    tls_thread_status.mem_consume(mem_size);
#else
    int64_t mem_size = data.size;
#endif

    auto deleter = [](const starrocks::CacheKey& key, void* value) { delete[] (uint8_t*)value; };

    ObjectCacheWriteOptions options;
    options.priority = in_memory ? 1 : 0;
    ObjectCacheHandle* obj_handle = nullptr;
    // Use mem size managed by memory allocator as this record charge size.
    // At the same time, we should record this record size for data fetching when lookup.
    Status st = _cache->insert(key.encode(), data.data, data.size, mem_size, deleter, &obj_handle, &options);
    if (st.ok()) {
        *handle = PageCacheHandle(_cache, obj_handle);
    }
    return st;
}

} // namespace starrocks
