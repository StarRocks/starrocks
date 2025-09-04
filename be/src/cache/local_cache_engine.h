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

#include "cache/block_cache/io_buffer.h"
#include "cache/cache_options.h"
#include "cache/object_cache/cache_types.h"
#include "common/status.h"

namespace starrocks {

enum class LocalCacheEngineType { STARCACHE, LRUCACHE };

class LocalCacheEngine {
public:
    virtual ~LocalCacheEngine() = default;

    virtual bool is_initialized() const = 0;

    // Write data to cache
    virtual Status write(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) = 0;

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned.
    virtual Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                        ReadCacheOptions* options) = 0;

    // Insert object to cache
    virtual Status insert(const std::string& key, void* value, size_t size, ObjectCacheDeleter deleter,
                          ObjectCacheHandlePtr* handle, const ObjectCacheWriteOptions& options) = 0;

    // Lookup object from cache, the `handle` wraps the object pointer.
    // As long as the handle object is not destroyed and the user does not manually call the `handle->release()`
    // function, the corresponding pointer will never be freed by the cache system.
    virtual Status lookup(const std::string& key, ObjectCacheHandlePtr* handle,
                          ObjectCacheReadOptions* options = nullptr) = 0;

    // Release a handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    virtual void release(ObjectCacheHandlePtr handle) = 0;

    // Return the value in the given handle returned by a previous insert() or lookup().
    // The handle must have not been released yet.
    virtual const void* value(ObjectCacheHandlePtr handle) = 0;

    virtual bool exist(const std::string& key) const = 0;

    // Remove data from cache.
    virtual Status remove(const std::string& key) = 0;

    // Adjust the cache quota by delta bytes.
    // If the new capacity is less than `min_capacity`, skip adjusting it.
    virtual Status adjust_mem_quota(int64_t delta, size_t min_capacity) = 0;

    // Update the datacache memory quota.
    virtual Status update_mem_quota(size_t quota_bytes, bool flush_to_disk) = 0;

    // Update the datacache disk space information, such as disk quota or disk path.
    virtual Status update_disk_spaces(const std::vector<DirSpace>& spaces) = 0;

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

    // Get the lookup count, including cache hit count and cache miss count.
    virtual size_t lookup_count() const = 0;

    // Get the cache hit count.
    virtual size_t hit_count() const = 0;

    // Get all cache metrics together.
    virtual const ObjectCacheMetrics metrics() const = 0;

    // Remove all cache entries that are not actively in use.
    virtual Status prune() = 0;
};

} // namespace starrocks
