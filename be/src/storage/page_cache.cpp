// This file is made available under Elastic License 2.0.
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

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

UIntGauge g_cache_size(MetricUnit::BYTES); // NOLINT

[[maybe_unused]] static void update_cache_size() {
    StoragePageCache::instance()->update_memory_usage_statistics();
}

StoragePageCache* StoragePageCache::_s_instance = nullptr;

void StoragePageCache::create_global_cache(MemTracker* mem_tracker, size_t capacity) {
    if (_s_instance == nullptr) {
        _s_instance = new StoragePageCache(mem_tracker, capacity);
#ifndef BE_TEST
        MetricRegistry* reg = StarRocksMetrics::instance()->metrics();
        reg->register_hook("page_cache_size_hook", update_cache_size);
        reg->register_metric("storage_page_cache_bytes", &g_cache_size);
#endif
    }
}

void StoragePageCache::release_global_cache() {
    if (_s_instance != nullptr) {
        delete _s_instance;
        _s_instance = nullptr;
    }
}

void StoragePageCache::update_memory_usage_statistics() {
    int64_t mem_usage = memory_usage();
    g_cache_size.set_value(mem_usage);
    _mem_tracker->consume(mem_usage - _mem_tracker->consumption());
}

StoragePageCache::StoragePageCache(MemTracker* mem_tracker, size_t capacity)
        : _mem_tracker(mem_tracker), _cache(new_lru_cache(capacity)) {}

StoragePageCache::~StoragePageCache() {
    _mem_tracker->release(_mem_tracker->consumption());
}

bool StoragePageCache::lookup(const CacheKey& key, PageCacheHandle* handle) {
    auto* lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = PageCacheHandle(_cache.get(), lru_handle);
    return true;
}

void StoragePageCache::insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle, bool in_memory) {
    auto deleter = [](const starrocks::CacheKey& key, void* value) { delete[](uint8_t*) value; };

    CachePriority priority = CachePriority::NORMAL;
    if (in_memory) {
        priority = CachePriority::DURABLE;
    }

    auto* lru_handle = _cache->insert(key.encode(), data.data, data.size, deleter, priority);
    *handle = PageCacheHandle(_cache.get(), lru_handle);
}

} // namespace starrocks
