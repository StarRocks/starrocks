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

#include "cache/lrucache_engine.h"

#include <butil/fast_rand.h>

namespace starrocks {
Status LRUCacheEngine::init(const CacheOptions& options) {
    _cache = std::make_unique<ShardedLRUCache>(options.mem_space_size);
    _initialized.store(true, std::memory_order_relaxed);
    return Status::OK();
}

Status LRUCacheEngine::write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) {
    return Status::NotSupported("LRUCache engine don't support write block");
}

Status LRUCacheEngine::read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                            ReadCacheOptions* options) {
    return Status::NotSupported("LRUCache engine don't support read block");
}

Status LRUCacheEngine::insert(const std::string& key, void* value, size_t size, ObjectCacheDeleter deleter,
                              ObjectCacheHandlePtr* handle, const ObjectCacheWriteOptions& options) {
    if (!_check_write(size, options)) {
        return Status::InternalError("cache insertion is rejected");
    }
    auto* lru_handle = _cache->insert(key, value, size, deleter, static_cast<CachePriority>(options.priority));
    if (handle) {
        *handle = reinterpret_cast<ObjectCacheHandlePtr>(lru_handle);
    }
    return Status::OK();
}

Status LRUCacheEngine::lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) {
    auto* lru_handle = _cache->lookup(CacheKey(key));
    if (!lru_handle) {
        return Status::NotFound("no such entry");
    }
    *handle = reinterpret_cast<ObjectCacheHandlePtr>(lru_handle);
    return Status::OK();
}

bool LRUCacheEngine::exist(const std::string& key) const {
    auto* handle = _cache->lookup(CacheKey(key));
    if (!handle) {
        return false;
    } else {
        _cache->release(handle);
        return true;
    }
}

Status LRUCacheEngine::remove(const std::string& key) {
    _cache->erase(CacheKey(key));
    return Status::OK();
}

Status LRUCacheEngine::update_mem_quota(size_t quota_bytes, bool flush_to_disk) {
    _cache->set_capacity(quota_bytes);
    return Status::OK();
}

Status LRUCacheEngine::update_disk_spaces(const std::vector<DirSpace>& spaces) {
    return Status::NotSupported("LRUCache engine don't support update disk spaces");
}

Status LRUCacheEngine::update_inline_cache_count_limit(int32_t limit) {
    return Status::NotSupported("LRUCache engine don't support update inline cache count limit");
}

const DataCacheMetrics LRUCacheEngine::cache_metrics() const {
    return DataCacheMetrics{.status = DataCacheStatus::NORMAL,
                            .mem_quota_bytes = _cache->get_capacity(),
                            .mem_used_bytes = _cache->get_memory_usage(),
                            .disk_quota_bytes = 0,
                            .disk_used_bytes = 0,
                            .meta_used_bytes = 0};
}

Status LRUCacheEngine::shutdown() {
    (void)_cache->prune();
    return Status::OK();
}

Status LRUCacheEngine::prune() {
    _cache->prune();
    return Status::OK();
}

void LRUCacheEngine::release(ObjectCacheHandlePtr handle) {
    auto lru_handle = reinterpret_cast<Cache::Handle*>(handle);
    _cache->release(lru_handle);
}

const void* LRUCacheEngine::value(ObjectCacheHandlePtr handle) {
    auto lru_handle = reinterpret_cast<Cache::Handle*>(handle);
    return _cache->value(lru_handle);
}

Status LRUCacheEngine::adjust_mem_quota(int64_t delta, size_t min_capacity) {
    if (_cache->adjust_capacity(delta, min_capacity)) {
        return Status::OK();
    }
    return Status::InternalError("adjust quota failed");
}

size_t LRUCacheEngine::mem_quota() const {
    return _cache->get_capacity();
}

size_t LRUCacheEngine::mem_usage() const {
    return _cache->get_memory_usage();
}

size_t LRUCacheEngine::lookup_count() const {
    return _cache->get_lookup_count();
}

size_t LRUCacheEngine::hit_count() const {
    return _cache->get_hit_count();
}

const ObjectCacheMetrics LRUCacheEngine::metrics() const {
    ObjectCacheMetrics m;
    m.capacity = _cache->get_capacity();
    m.usage = _cache->get_memory_usage();
    m.lookup_count = _cache->get_lookup_count();
    m.hit_count = _cache->get_hit_count();
    // Unsupported
    m.object_item_count = 0;
    return m;
}

bool LRUCacheEngine::_check_write(size_t charge, const ObjectCacheWriteOptions& options) const {
    if (options.evict_probability >= 100) {
        return true;
    }
    if (options.evict_probability <= 0) {
        return false;
    }

    /*
    // TODO: The cost of this call may be relatively high, and it needs to be optimized later.
    if (_cache->get_memory_usage() + charge <= _cache->get_capacity()) {
        return true;
    }
    */

    if (butil::fast_rand_less_than(100) < options.evict_probability) {
        return true;
    }
    return false;
}

} // namespace starrocks