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

#include "cache/mem_cache/page_cache.h"

#include <malloc.h>

#include "base/container/lru_cache.h"
#include "base/utility/defer_op.h"
#include "runtime/current_thread.h"

namespace starrocks {

std::atomic<size_t> StoragePageCacheMetrics::returned_page_handle_count{};
std::atomic<size_t> StoragePageCacheMetrics::released_page_handle_count{};

PageCacheMetrics* PageCacheMetrics::instance() {
    // Process-lifetime singleton: registered Metric objects keep back-pointers
    // to MetricRegistry, so avoid exit-time destruction after registry teardown.
    static auto* instance = new PageCacheMetrics();
    return instance;
}

void PageCacheMetrics::install(MetricRegistry* registry, StoragePageCache* cache) {
    if (_registry != nullptr) {
        DCHECK_EQ(_registry, registry);
        _set_cache(cache);
        return;
    }
    _registry = registry;
    _set_cache(cache);

    registry->register_metric("page_cache_lookup_count", &page_cache_lookup_count);
    registry->register_hook("page_cache_lookup_count", [] { PageCacheMetrics::instance()->_update_lookup_count(); });

    registry->register_metric("page_cache_hit_count", &page_cache_hit_count);
    registry->register_hook("page_cache_hit_count", [] { PageCacheMetrics::instance()->_update_hit_count(); });

    registry->register_metric("page_cache_insert_count", &page_cache_insert_count);
    registry->register_hook("page_cache_insert_count", [] { PageCacheMetrics::instance()->_update_insert_count(); });

    registry->register_metric("page_cache_insert_evict_count", &page_cache_insert_evict_count);
    registry->register_hook("page_cache_insert_evict_count",
                            [] { PageCacheMetrics::instance()->_update_insert_evict_count(); });

    registry->register_metric("page_cache_release_evict_count", &page_cache_release_evict_count);
    registry->register_hook("page_cache_release_evict_count",
                            [] { PageCacheMetrics::instance()->_update_release_evict_count(); });

    registry->register_metric("page_cache_capacity", &page_cache_capacity);
    registry->register_hook("page_cache_capacity", [] { PageCacheMetrics::instance()->_update_capacity(); });

    registry->register_metric("page_cache_pinned_count", &page_cache_pinned_count);
    registry->register_hook("page_cache_pinned_count", [] { PageCacheMetrics::instance()->_update_pinned_count(); });
}

void PageCacheMetrics::_update_lookup_count() {
    page_cache_lookup_count.set_value(_cache == nullptr ? 0 : _cache->get_lookup_count());
}

void PageCacheMetrics::_update_hit_count() {
    page_cache_hit_count.set_value(_cache == nullptr ? 0 : _cache->get_hit_count());
}

void PageCacheMetrics::_update_insert_count() {
    page_cache_insert_count.set_value(_cache == nullptr ? 0 : _cache->get_insert_count());
}

void PageCacheMetrics::_update_insert_evict_count() {
    page_cache_insert_evict_count.set_value(_cache == nullptr ? 0 : _cache->get_insert_evict_count());
}

void PageCacheMetrics::_update_release_evict_count() {
    page_cache_release_evict_count.set_value(_cache == nullptr ? 0 : _cache->get_release_evict_count());
}

void PageCacheMetrics::_update_capacity() {
    page_cache_capacity.set_value(_cache == nullptr ? 0 : _cache->get_capacity());
}

void PageCacheMetrics::_update_pinned_count() {
    page_cache_pinned_count.set_value(_cache == nullptr ? 0 : _cache->get_pinned_count());
}

void StoragePageCache::init_metrics(MetricRegistry* metrics) {
    if (metrics != nullptr) {
        PageCacheMetrics::instance()->install(metrics, this);
    }
}

void StoragePageCache::prune() {
    (void)_cache->prune();
}

void StoragePageCache::set_capacity(size_t capacity) {
    Status st = _cache->update_mem_quota(capacity);
    LOG_IF(INFO, !st.ok()) << "Fail to set cache capacity to " << capacity << ", reason: " << st.message();
}

size_t StoragePageCache::get_capacity() const {
    return _cache->mem_quota();
}

uint64_t StoragePageCache::get_lookup_count() const {
    return _cache->lookup_count();
}

uint64_t StoragePageCache::get_hit_count() const {
    return _cache->hit_count();
}

uint64_t StoragePageCache::get_insert_count() const {
    return _cache->insert_count();
}

uint64_t StoragePageCache::get_insert_evict_count() const {
    return _cache->insert_evict_count();
}

uint64_t StoragePageCache::get_release_evict_count() const {
    return _cache->release_evict_count();
}

bool StoragePageCache::adjust_capacity(int64_t delta, size_t min_capacity) {
    Status st = _cache->adjust_mem_quota(delta, min_capacity);
    if (!st.ok()) {
        LOG_IF(INFO, !st.ok()) << "Fail to adjust cache capacity, delta: " << delta << ", reason: " << st.message();
        return false;
    }
    return true;
}

size_t StoragePageCache::get_pinned_count() const {
    return StoragePageCacheMetrics::returned_page_handle_count - StoragePageCacheMetrics::released_page_handle_count;
}

bool StoragePageCache::lookup(const std::string& key, PageCacheHandle* handle) {
    MemCacheHandle* obj_handle = nullptr;
    Status st = _cache->lookup(key, &obj_handle);
    if (!st.ok()) {
        return false;
    }
    StoragePageCacheMetrics::returned_page_handle_count++;
    *handle = PageCacheHandle(_cache, obj_handle);
    return true;
}

Status StoragePageCache::insert(const std::string& key, std::vector<uint8_t>* data, const MemCacheWriteOptions& opts,
                                PageCacheHandle* handle) {
#ifndef BE_TEST
    int64_t mem_size = malloc_usable_size(data->data()) + sizeof(*data);
    tls_thread_status.mem_release(mem_size);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    tls_thread_status.mem_consume(mem_size);
#else
    int64_t mem_size = data->capacity() + sizeof(*data);
#endif

    auto deleter = [](const starrocks::CacheKey& key, void* value) {
        auto* cache_item = (std::vector<uint8_t>*)value;
        delete cache_item;
    };

    MemCacheHandle* obj_handle = nullptr;
    // Use mem size managed by memory allocator as this record charge size.
    // At the same time, we should record this record size for data fetching when lookup.
    Status st = _cache->insert(key, (void*)data, mem_size, deleter, &obj_handle, opts);
    if (st.ok()) {
        *handle = PageCacheHandle(_cache, obj_handle);
        StoragePageCacheMetrics::returned_page_handle_count++;
    }
    return st;
}

Status StoragePageCache::insert(const std::string& key, void* data, int64_t size, MemCacheDeleter deleter,
                                const MemCacheWriteOptions& opts, PageCacheHandle* handle) {
    MemCacheHandle* obj_handle = nullptr;
    Status st = _cache->insert(key, data, size, deleter, &obj_handle, opts);
    if (st.ok()) {
        *handle = PageCacheHandle(_cache, obj_handle);
        StoragePageCacheMetrics::returned_page_handle_count++;
    }
    return st;
}

} // namespace starrocks
