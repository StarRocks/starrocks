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

#include "tenann/index/index_cache.h"
#include "util/dynamic_cache.h"

namespace starrocks {

// VectorIndexCacheEntry holds the cached tenann::IndexRef. All access to _ref
// goes through guard() — read or write. This matches PrimaryIndex /
// StoragePageCache and keeps set_ref + update_object_size paired and
// single-flights the cold-load path in VectorIndexCache::GetOrCreate.
class VectorIndexCacheEntry {
public:
    std::unique_lock<std::mutex> guard() { return std::unique_lock<std::mutex>(_mu); }

    // All of the below must be called while holding guard().
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

// SR-owned tenann::IndexCache backed by DynamicCache. MemTracker is attached
// on construction; ExecEnv must destruct this before the tracker hierarchy
// (see ExecEnv::destroy).
class VectorIndexCache final : public tenann::IndexCache {
public:
    using Cache = DynamicCache<std::string, VectorIndexCacheEntry>;
    using Entry = Cache::Entry;

    VectorIndexCache(size_t capacity, MemTracker* tracker);
    ~VectorIndexCache() override;

    VectorIndexCache(const VectorIndexCache&) = delete;
    VectorIndexCache& operator=(const VectorIndexCache&) = delete;

    [[nodiscard]] bool Lookup(const tenann::CacheKey& key, tenann::IndexCacheHandle* handle) override;
    // tenann::IndexCache contract; SR production paths all go through
    // GetOrCreate. Currently only exercised by the unit tests as a direct
    // way to populate entries (LRU / capacity / mem-tracker coverage).
    void Insert(const tenann::CacheKey& key, tenann::IndexRef ref, tenann::IndexCacheHandle* handle) override;
    // Returns false if the loader threw or returned null; callers must check
    // the bool, not wrap in try/catch.
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

// Stub used in non-tenann builds (e.g. Darwin). The BE never constructs a
// VectorIndexCache when tenann is not linked, so this stub is only here so
// callers can hold a `VectorIndexCache*` and reference its stat accessors
// without their own WITH_TENANN guards. All public methods return zero/no-op
// values, matching the "no cache exists" semantics those callers handle via
// the nullptr check on ExecEnv::vector_index_cache().
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
