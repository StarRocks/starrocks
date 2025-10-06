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

#include "cache/cache_metrics.h"
#include "cache/disk_cache/io_buffer.h"
#include "common/status.h"

namespace starrocks {
class CacheKey;

struct MemCacheOptions {
    size_t mem_space_size = 0;
};

struct MemCacheWriteOptions {
    // The priority of the cache object, only support 0 and 1 now.
    int8_t priority = 0;

    // The probability to evict other items if the cache space is full, which can help avoid frequent cache replacement
    // and improve cache hit rate sometimes.
    // It is expressed as a percentage. If evict_probability is 10, it means the probability to evict other data is 10%.
    int32_t evict_probability = 100;
};

struct MemCacheReadOptions {};

struct MemCacheHandle {};

using MemCacheHandlePtr = MemCacheHandle*;

// using CacheDeleter = std::function<void(const std::string&, void*)>;
//
// We only use the deleter function of the lru cache temporarily.
// Maybe a std::function object or a function pointer like `void (*)(std::string&, void*)` which
// independent on lru cache is more appropriate, but it is not easy to convert them to the lru
// cache deleter when using a lru cache module.
using MemCacheDeleter = void (*)(const CacheKey&, void*);

struct DataCacheMemMetrics {
    size_t mem_quota_bytes = 0;
    size_t mem_used_bytes = 0;
};

class LocalMemCacheEngine {
public:
    virtual ~LocalMemCacheEngine() = default;

    virtual bool is_initialized() const = 0;

    // Insert object to cache
    virtual Status insert(const std::string& key, void* value, size_t size, MemCacheDeleter deleter,
                          MemCacheHandlePtr* handle, const MemCacheWriteOptions& options) = 0;

    // Lookup object from cache, the `handle` wraps the object pointer.
    // As long as the handle object is not destroyed and the user does not manually call the `handle->release()`
    // function, the corresponding pointer will never be freed by the cache system.
    virtual Status lookup(const std::string& key, MemCacheHandlePtr* handle,
                          MemCacheReadOptions* options = nullptr) = 0;

    // Release a handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    virtual void release(MemCacheHandlePtr handle) = 0;

    // Return the value in the given handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    virtual const void* value(MemCacheHandlePtr handle) = 0;

    virtual bool exist(const std::string& key) const = 0;

    // Remove data from cache.
    virtual Status remove(const std::string& key) = 0;

    // Adjust the cache quota by delta bytes.
    // If the new capacity is less than `min_capacity`, skip adjusting it.
    virtual Status adjust_mem_quota(int64_t delta, size_t min_capacity) = 0;

    // Update the datacache memory quota.
    virtual Status update_mem_quota(size_t quota_bytes) = 0;

    virtual const DataCacheMemMetrics cache_metrics() const = 0;

    virtual Status shutdown() = 0;

    virtual bool has_mem_cache() const = 0;
    virtual bool available() const = 0;
    virtual bool mem_cache_available() const = 0;

    virtual size_t mem_quota() const = 0;
    virtual size_t mem_usage() const = 0;

    // Get the lookup count, including cache hit count and cache miss count.
    virtual size_t lookup_count() const = 0;

    // Get the cache hit count.
    virtual size_t hit_count() const = 0;

    // Remove all cache entries that are not actively in use.
    virtual Status prune() = 0;
};

} // namespace starrocks