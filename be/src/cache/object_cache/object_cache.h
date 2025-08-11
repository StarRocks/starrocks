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

#include <butil/macros.h>

#include <atomic>

#include "cache/object_cache/cache_types.h"
#include "common/status.h"
#include "util/lru_cache.h"

namespace starrocks {

class ObjectCache {
public:
    virtual ~ObjectCache() = default;

    // Insert object to cache, the `ptr` is the object pointer.
    // size: the size of the value.
    // charge: the actual memory size allocated for this value.
    virtual Status insert(const std::string& key, void* value, size_t size, size_t charge, ObjectCacheDeleter deleter,
                          ObjectCacheHandlePtr* handle, ObjectCacheWriteOptions* options = nullptr) = 0;

    // Lookup object from cache, the `handle` wraps the object pointer.
    // As long as the handle object is not destroyed and the user does not manully call the `handle->release()`
    // function, the corresponding pointer will never be freed by the cache system.
    virtual Status lookup(const std::string& key, ObjectCacheHandlePtr* handle,
                          ObjectCacheReadOptions* options = nullptr) = 0;

    // Remove object from cache. If the object not exist, return Status::Not_Found.
    virtual Status remove(const std::string& key) = 0;

    // Release a handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    virtual void release(ObjectCacheHandlePtr handle) = 0;

    // Return the value in the given handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    virtual const void* value(ObjectCacheHandlePtr handle) = 0;

    // Return the value in Slice format encapsulated in the given handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    virtual Slice value_slice(ObjectCacheHandlePtr handle) = 0;

    // Adjust the cache quota by delta bytes.
    // If the new capacity is less than `min_capacity`, skip adjusting it.
    virtual Status adjust_capacity(int64_t delta, size_t min_capacity) = 0;

    // Set the cache capacity to a target value.
    virtual Status set_capacity(size_t capacity) = 0;

    // Get the cache capacity.
    virtual size_t capacity() const = 0;

<<<<<<< HEAD:be/src/cache/object_cache/object_cache.h
    // Get the memory usage.
    virtual size_t usage() const = 0;
=======
    // Update the datacache inline cache count limit
    virtual Status update_inline_cache_count_limit(int32_t limit) = 0;

    virtual const DataCacheMetrics cache_metrics() const = 0;

    virtual void record_read_remote(size_t size, int64_t latency_us) = 0;

    virtual void record_read_cache(size_t size, int64_t latency_us) = 0;

    virtual Status shutdown() = 0;

    virtual LocalCacheEngineType engine_type() = 0;

    virtual bool has_mem_cache() const = 0;
    virtual bool has_disk_cache() const = 0;
    virtual bool available() const = 0;
    virtual bool mem_cache_available() const = 0;
    virtual void disk_spaces(std::vector<DirSpace>* spaces) const = 0;

    virtual size_t mem_quota() const = 0;
    virtual size_t mem_usage() const = 0;
>>>>>>> ac8a74a78e ([BugFix] support configure starcache inline cache count limit (#61724)):be/src/cache/local_cache_engine.h

    // Get the lookup count, including cache hit count and cache miss count.
    virtual size_t lookup_count() const = 0;

    // Get the cache hit count.
    virtual size_t hit_count() const = 0;

    // Get all cache metrics together.
    virtual const ObjectCacheMetrics metrics() const = 0;

    // Remove all cache entries that are not actively in use.
    virtual Status prune() = 0;

    // Shutdown the cache instance to save some state meta.
    virtual Status shutdown() = 0;

    bool initialized() const { return _initialized.load(std::memory_order_relaxed); }

    bool available() const { return initialized() && capacity() > 0; }

protected:
    std::atomic<bool> _initialized = false;
};

} // namespace starrocks