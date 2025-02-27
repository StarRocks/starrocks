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

#include "cache/object_cache/lrucache_module.h"

#include <butil/fast_rand.h>

#include "common/logging.h"

namespace starrocks {

LRUCacheModule::~LRUCacheModule() {
    if (_cache) {
        _cache->prune();
    }
}

Status LRUCacheModule::init() {
    if (!_cache) {
        _cache.reset(new_lru_cache(_options.capacity));
    }
    return Status::OK();
}

Status LRUCacheModule::insert(const std::string& key, void* value, size_t size, size_t charge,
                              ObjectCacheDeleter deleter, ObjectCacheHandlePtr* handle,
                              ObjectCacheWriteOptions* options) {
    if (!_check_write(charge, options)) {
        return Status::InternalError("cache insertion is rejected");
    }
    auto* lru_handle = _cache->insert(key, value, size, charge, deleter, static_cast<CachePriority>(options->priority));
    if (handle) {
        *handle = reinterpret_cast<ObjectCacheHandlePtr>(lru_handle);
    }
    return Status::OK();
}

Status LRUCacheModule::lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options) {
    auto* lru_handle = _cache->lookup(CacheKey(key));
    if (!lru_handle) {
        return Status::NotFound("no such entry");
    }
    *handle = reinterpret_cast<ObjectCacheHandlePtr>(lru_handle);
    return Status::OK();
}

Status LRUCacheModule::remove(const std::string& key) {
    _cache->erase(CacheKey(key));
    return Status::OK();
}

void LRUCacheModule::release(ObjectCacheHandlePtr handle) {
    auto lru_handle = reinterpret_cast<Cache::Handle*>(handle);
    _cache->release(lru_handle);
}

const void* LRUCacheModule::value(ObjectCacheHandlePtr handle) {
    auto lru_handle = reinterpret_cast<Cache::Handle*>(handle);
    return _cache->value(lru_handle);
}

Slice LRUCacheModule::value_slice(ObjectCacheHandlePtr handle) {
    auto lru_handle = reinterpret_cast<Cache::Handle*>(handle);
    return _cache->value_slice(lru_handle);
}

Status LRUCacheModule::adjust_capacity(int64_t delta, size_t min_capacity) {
    if (_cache->adjust_capacity(delta, min_capacity)) {
        return Status::OK();
    }
    return Status::InternalError("adjust quota failed");
}

Status LRUCacheModule::set_capacity(size_t capacity) {
    _cache->set_capacity(capacity);
    return Status::OK();
}

size_t LRUCacheModule::capacity() const {
    return _cache->get_capacity();
}

size_t LRUCacheModule::usage() const {
    return _cache->get_memory_usage();
}

size_t LRUCacheModule::lookup_count() const {
    return _cache->get_lookup_count();
}

size_t LRUCacheModule::hit_count() const {
    return _cache->get_hit_count();
}

const ObjectCacheMetrics LRUCacheModule::metrics() const {
    ObjectCacheMetrics m;
    m.capacity = _cache->get_capacity();
    m.usage = _cache->get_memory_usage();
    m.lookup_count = _cache->get_lookup_count();
    m.hit_count = _cache->get_hit_count();
    // Unsupported
    m.object_item_count = 0;
    return m;
}

Status LRUCacheModule::prune() {
    _cache->prune();
    return Status::OK();
}

Status LRUCacheModule::shutdown() {
    _cache->prune();
    return Status::OK();
}

bool LRUCacheModule::_check_write(size_t charge, ObjectCacheWriteOptions* options) const {
    if (options->evict_probability >= 100) {
        return true;
    }
    if (options->evict_probability <= 0) {
        return false;
    }

    // TODO: The cost of this call may be relatively high, and it needs to be optimized later.
    if (_cache->get_memory_usage() + charge <= _cache->get_capacity()) {
        return true;
    }

    if (butil::fast_rand_less_than(100) < options->evict_probability) {
        return true;
    }
    return false;
}

} // namespace starrocks