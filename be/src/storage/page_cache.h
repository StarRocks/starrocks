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

#include "gutil/macros.h" // for DISALLOW_COPY
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"
#include "util/lru_cache.h"

namespace starrocks {

class PageCacheHandle;
class MemTracker;

// Page cache min size is 256MB
static constexpr int64_t kcacheMinSize = 268435456;

// Warpper around Cache, and used for cache page of column datas
// in Segment.
// TODO(zc): We should add some metric to see cache hit/miss rate.
class StoragePageCache {
public:
    virtual ~StoragePageCache();
    // The unique key identifying entries in the page cache.
    // Each cached page corresponds to a specific offset within
    // a file.
    //
    // TODO(zc): Now we use file name(std::string) as a part of
    // key, which is not efficient. We should make it better later
    struct CacheKey {
        CacheKey(std::string fname_, int64_t offset_) : fname(std::move(fname_)), offset(offset_) {}
        std::string fname;
        int64_t offset;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            std::string key_buf(fname);
            key_buf.append((char*)&offset, sizeof(offset));
            return key_buf;
        }
    };

    // Create global instance of this class
    static void create_global_cache(MemTracker* mem_tracker, size_t capacity);

    static void release_global_cache();

    // Return global instance.
    // Client should call create_global_cache before.
    static StoragePageCache* instance() { return _s_instance; }

    StoragePageCache(MemTracker* mem_tracker, size_t capacity);

    // Lookup the given page in the cache.
    //
    // If the page is found, the cache entry will be written into handle.
    // PageCacheHandle will release cache entry to cache when it
    // destructs.
    //
    // Return true if entry is found, otherwise return false.
    bool lookup(const CacheKey& key, PageCacheHandle* handle);

    // Insert a page with key into this cache.
    // Given hanlde will be set to valid reference.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    void insert(const CacheKey& key, const Slice& data, PageCacheHandle* handle, bool in_memory = false);

    size_t memory_usage() const { return _cache->get_memory_usage(); }

    void set_capacity(size_t capacity);

    size_t get_capacity();

    uint64_t get_lookup_count();

    uint64_t get_hit_count();

    bool adjust_capacity(int64_t delta, size_t min_capacity = 0);

private:
    static StoragePageCache* _s_instance;

    MemTracker* _mem_tracker = nullptr;
    std::unique_ptr<Cache> _cache = nullptr;
};

// A handle for StoragePageCache entry. This class make it easy to handle
// Cache entry. Users don't need to release the obtained cache entry. This
// class will release the cache entry when it is destroyed.
class PageCacheHandle {
public:
    PageCacheHandle() = default;
    PageCacheHandle(Cache* cache, Cache::Handle* handle) : _cache(cache), _handle(handle) {}
    ~PageCacheHandle() {
        if (_handle != nullptr) {
#ifndef BE_TEST
            MemTracker* prev_tracker =
                    tls_thread_status.set_mem_tracker(ExecEnv::GetInstance()->page_cache_mem_tracker());
            DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });
#endif
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

    Cache* cache() const { return _cache; }
    Slice data() const { return _cache->value_slice(_handle); }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    PageCacheHandle(const PageCacheHandle&) = delete;
    const PageCacheHandle& operator=(const PageCacheHandle&) = delete;
};

} // namespace starrocks
