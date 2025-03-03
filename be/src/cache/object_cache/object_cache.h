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

#include "cache/object_cache/cache_module.h"
#include "common/status.h"
#include "util/lru_cache.h"

namespace starrocks {

class ObjectCache {
public:
    ObjectCache() = default;
    ~ObjectCache();

    // Init the object cache instance with options.
    Status init(const ObjectCacheOptions& options);

    // Insert object to cache, the `ptr` is the object pointer.
    // size: the size of the value.
    // charge: the actual memory size mallocated for this value.
    Status insert(const std::string& key, void* value, size_t size, size_t charge, ObjectCacheDeleter deleter,
                  ObjectCacheHandlePtr* handle, ObjectCacheWriteOptions* options = nullptr);

    // Lookup object from cache, the `handle` wraps the object pointer.
    // As long as the handle object is not destroyed and the user does not manully call the `handle->release()`
    // function, the corresponding pointer will never be freed by the cache system.
    Status lookup(const std::string& key, ObjectCacheHandlePtr* handle, ObjectCacheReadOptions* options = nullptr);

    // Remove object from cache. If the object not exist, return Status::Not_Found.
    Status remove(const std::string& key);

    // Release a handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    void release(ObjectCacheHandlePtr handle);

    // Return the value in the given handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    const void* value(ObjectCacheHandlePtr handle);

    // Return the value in Slice format encapsulated in the given handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    Slice value_slice(ObjectCacheHandlePtr handle);

    // Adjust the cache quota by delta bytes.
    // If the new capcity is less than `min_capacity`, skip adjusting it.
    Status adjust_capacity(int64_t delta, size_t min_capacity);

    // Set the cache capacity to a target value.
    Status set_capacity(size_t capacity);

    // Get the cache capacity.
    size_t capacity() const;

    // Get the memory usage.
    size_t usage() const;

    // Get the lookup count, including cache hit count and cache miss count.
    size_t lookup_count() const;

    // Get the cache hit count.
    size_t hit_count() const;

    // Get all cache metrics together.
    const ObjectCacheMetrics metrics() const;

    // Remove all cache entries that are not actively in use.
    Status prune();

    // Shutdown the cache instance to save some state meta.
    Status shutdown();

    bool initialized() const { return _initialized.load(std::memory_order_relaxed); }

    bool available() const { return initialized() && capacity() > 0; }

private:
    std::shared_ptr<ObjectCacheModule> _cache_module;
    std::atomic<bool> _initialized = false;
};

} // namespace starrocks