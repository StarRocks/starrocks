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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/page_cache.h

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

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "cache/datacache.h"
#include "gutil/macros.h" // for DISALLOW_COPY
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace starrocks {

class PageCacheHandle;
class MemTracker;
class ObjectCacheWriteOptions;

// Page cache min size is 256MB
static constexpr int64_t kcacheMinSize = 268435456;

// Wrapper around Cache, and used for cache page of column datas in Segment.
// TODO(zc): We should add some metric to see cache hit/miss rate.
class StoragePageCache {
public:
    StoragePageCache() = default;
    virtual ~StoragePageCache() = default;

    void init_metrics();

    // Return global instance.
    // Client should call create_global_cache before.
    static StoragePageCache* instance() { return DataCache::GetInstance()->page_cache(); }

    StoragePageCache(LocalMemCacheEngine* cache_engine) : _cache(cache_engine), _initialized(true) {}

    void init(LocalMemCacheEngine* cache_engine) {
        _cache = cache_engine;
        _initialized.store(true, std::memory_order_relaxed);
    }

    // Lookup the given page in the cache.
    //
    // If the page is found, the cache entry will be written into handle.
    // PageCacheHandle will release cache entry to cache when it
    // destructs.
    //
    // Return true if entry is found, otherwise return false.
    bool lookup(const std::string& key, PageCacheHandle* handle);

    // Insert a page with key into this cache.
    // Given handle will be set to valid reference.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    Status insert(const std::string& key, std::vector<uint8_t>* data, const ObjectCacheWriteOptions& opts,
                  PageCacheHandle* handle);

    Status insert(const std::string& key, void* data, int64_t size, ObjectCacheDeleter deleter,
                  const ObjectCacheWriteOptions& opts, PageCacheHandle* handle);

    size_t memory_usage() const { return _cache->mem_usage(); }

    void set_capacity(size_t capacity);

    size_t get_capacity() const;

    uint64_t get_lookup_count() const;

    uint64_t get_hit_count() const;

    bool adjust_capacity(int64_t delta, size_t min_capacity = 0);

    void prune();

    bool is_initialized() const { return _initialized.load(std::memory_order_relaxed); }
    bool available() const { return is_initialized() && _cache->mem_cache_available(); }

private:
    LocalMemCacheEngine* _cache = nullptr;
    std::atomic<bool> _initialized = false;
};

// A handle for StoragePageCache entry. This class make it easy to handle
// Cache entry. Users don't need to release the obtained cache entry. This
// class will release the cache entry when it is destroyed.
class PageCacheHandle {
public:
    PageCacheHandle() = default;
    PageCacheHandle(LocalMemCacheEngine* cache, ObjectCacheHandle* handle) : _cache(cache), _handle(handle) {}

    // Don't allow copy and assign
    PageCacheHandle(const PageCacheHandle&) = delete;
    const PageCacheHandle& operator=(const PageCacheHandle&) = delete;

    ~PageCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    PageCacheHandle(PageCacheHandle&& other) noexcept {
        // we can use std::exchange if we switch c++14 on
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    PageCacheHandle& operator=(PageCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    LocalMemCacheEngine* cache() const { return _cache; }
    const void* data() const { return _cache->value(_handle); }

private:
    LocalMemCacheEngine* _cache = nullptr;
    ObjectCacheHandle* _handle = nullptr;
};

} // namespace starrocks
