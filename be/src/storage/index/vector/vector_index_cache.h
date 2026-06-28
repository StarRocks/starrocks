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

#include <cstddef>
#include <cstdint>

namespace starrocks {
class MemTracker;
}

#ifdef WITH_TENANN

#include <atomic>
#include <mutex>
#include <ostream>
#include <string>

#include "cache/dynamic_cache.h"
#include "tenann/index/index_cache.h"

namespace starrocks {

// Per-entry holder: access to _ref must be under guard() so set_ref pairs with
// update_object_size and single-flights the cold-load path in GetOrCreate.
class VectorIndexCacheEntry {
public:
    std::unique_lock<std::mutex> guard() { return std::unique_lock<std::mutex>(_mu); }

    bool has_ref() const { return _ref != nullptr; }
    tenann::IndexRef ref() const { return _ref; }
    void set_ref(tenann::IndexRef ref) { _ref = std::move(ref); }
    size_t memory_usage() const { return _ref ? _ref->EstimateMemoryUsage() : 0; }

private:
    std::mutex _mu;
    tenann::IndexRef _ref;
};

inline std::ostream& operator<<(std::ostream& os, const VectorIndexCacheEntry&) {
    return os << "VectorIndexCacheEntry";
}

// SR-owned tenann::IndexCache backed by DynamicCache, with MemTracker attached.
class VectorIndexCache final : public tenann::IndexCache {
public:
    using Cache = DynamicCache<std::string, VectorIndexCacheEntry>;
    using Entry = Cache::Entry;

    VectorIndexCache(size_t capacity, MemTracker* tracker);
    ~VectorIndexCache() override;

    VectorIndexCache(const VectorIndexCache&) = delete;
    VectorIndexCache& operator=(const VectorIndexCache&) = delete;

    [[nodiscard]] bool Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) override;
    // Required by tenann::IndexCache; SR production paths use GetOrCreate.
    void Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) override;
    // Returns false on loader failure (exception or null IndexRef).
    [[nodiscard]] bool GetOrCreate(const tenann::CacheKey& key, const IndexLoader& loader,
                                   tenann::IndexCacheHandle* handle) override;

    void SetCapacity(size_t new_capacity) { _cache.set_capacity(new_capacity); }
    size_t capacity() const { return _cache.capacity(); }
    size_t memory_usage() const { return _cache.size(); }

    uint64_t lookup_count() const { return _lookup_count.load(std::memory_order_relaxed); }
    uint64_t hit_count() const { return _hit_count.load(std::memory_order_relaxed); }

private:
    tenann::IndexCacheHandle _wrap(Entry* entry, tenann::IndexRef ref);

    Cache _cache;
    std::atomic<uint64_t> _lookup_count{0};
    std::atomic<uint64_t> _hit_count{0};
};

} // namespace starrocks

#else // !WITH_TENANN

namespace starrocks {

// Stub for non-tenann builds (e.g. Darwin): never constructed, lets callers
// reference accessors without their own WITH_TENANN guards.
class VectorIndexCache {
public:
    VectorIndexCache(size_t, MemTracker*) {}
    void SetCapacity(size_t) {}
    size_t capacity() const { return 0; }
    size_t memory_usage() const { return 0; }
    uint64_t lookup_count() const { return 0; }
    uint64_t hit_count() const { return 0; }
};

} // namespace starrocks

#endif // WITH_TENANN
