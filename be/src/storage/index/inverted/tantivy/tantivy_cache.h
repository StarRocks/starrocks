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

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/statusor.h"
#include "fs/fs.h"
#include "roaring/roaring.hh"
#include "storage/index/inverted/tantivy/tantivy_cache_key.h"
#include "storage/index/inverted/tantivy/tantivy_ffi_guards.h"

namespace starrocks {

class MemTracker;
class TantivyReadBufferPool;
template <class Key, class T, class Lock>
class DynamicCache;

struct TantivyReaderResource {
    ~TantivyReaderResource();

    TantivyIndexIdentity identity;
    roaring::Roaring null_bitmap;

    // Declaration order is a lifetime contract: PullDirectory stores a raw
    // pointer to ra_file, so reader must be destroyed first.
    std::unique_ptr<RandomAccessFile> ra_file;
    // Keeps the C++ backing pool alive until the Rust reader and all of its
    // OwnedBytes leases have been destroyed.
    std::shared_ptr<TantivyReadBufferPool> read_buffer_pool;
    TantivyReaderGuard reader;

    size_t estimated_bytes = 0;
    size_t materialized_bytes = 0;
    size_t resident_bytes = 0;
    uint64_t resident_read_count = 0;
    uint64_t resident_read_bytes = 0;
    bool resident_directory = false;
    size_t effective_charge = 0;
    uint32_t fd_charge = 0;
    MemTracker* cache_mem_tracker = nullptr;
    size_t tracked_bytes = 0;
};

struct TantivyReaderCacheStats {
    uint64_t lookup = 0;
    uint64_t hit = 0;
    uint64_t miss = 0;
    uint64_t bypass = 0;
    uint64_t insert = 0;
    uint64_t oversize_reject = 0;
    uint64_t build = 0;
    uint64_t build_error = 0;
    uint64_t duplicate_build_prevented = 0;
    uint64_t singleflight_waiters = 0;
    uint64_t singleflight_wait_ns = 0;
    size_t entries = 0;
    size_t estimated_resident_bytes = 0;
    size_t resident_directory_entries = 0;
    size_t resident_directory_bytes = 0;
};

struct TantivyQueryCacheStats {
    uint64_t lookup = 0;
    uint64_t hit = 0;
    uint64_t miss = 0;
    uint64_t bypass = 0;
    uint64_t insert = 0;
    uint64_t oversize_reject = 0;
    uint64_t key_too_large = 0;
    uint64_t ghost_record = 0;
    uint64_t ghost_admit = 0;
    size_t entries = 0;
    size_t estimated_resident_bytes = 0;
};

class TantivyReaderCache {
public:
    using ResourcePtr = std::shared_ptr<TantivyReaderResource>;
    using Loader = std::function<StatusOr<ResourcePtr>()>;
    using IsCancelled = std::function<bool()>;

    TantivyReaderCache(size_t capacity, size_t max_entries, size_t max_entry_bytes, MemTracker* mem_tracker);
    ~TantivyReaderCache();

    StatusOr<ResourcePtr> get_or_load(const TantivyIndexIdentity& identity, const Loader& loader,
                                      const IsCancelled& is_cancelled = {});
    ResourcePtr lookup(const TantivyIndexIdentity& identity);
    void record_bypass();
    void erase(const TantivyIndexIdentity& identity);
    void prune();
    void set_capacity(size_t capacity);
    bool would_admit(const TantivyIndexIdentity& identity, size_t estimated_bytes) const;

    size_t capacity() const;
    size_t memory_usage() const;
    TantivyReaderCacheStats stats() const;

private:
    struct CacheValue;
    using Cache = DynamicCache<std::string, CacheValue, std::mutex>;
    struct LoadState;
    struct LoadingStripe;

    ResourcePtr _lookup(const std::string& key, bool update_stats);
    void _maybe_insert(const std::string& key, const ResourcePtr& resource);
    bool _is_admissible(size_t estimated_bytes) const;
    std::unique_ptr<Cache> _cache;
    MemTracker* _mem_tracker;
    size_t _max_entries;
    size_t _max_entry_bytes;
    std::array<std::unique_ptr<LoadingStripe>, 64> _loading_stripes;

    std::atomic<uint64_t> _lookup_count{0};
    std::atomic<uint64_t> _hit_count{0};
    std::atomic<uint64_t> _miss_count{0};
    std::atomic<uint64_t> _bypass_count{0};
    std::atomic<uint64_t> _insert_count{0};
    std::atomic<uint64_t> _oversize_reject_count{0};
    std::atomic<uint64_t> _build_count{0};
    std::atomic<uint64_t> _build_error_count{0};
    std::atomic<uint64_t> _duplicate_build_prevented{0};
    std::atomic<uint64_t> _singleflight_waiters{0};
    std::atomic<uint64_t> _singleflight_wait_ns{0};
    std::atomic<size_t> _entries{0};
    std::atomic<size_t> _estimated_resident_bytes{0};
    std::atomic<size_t> _resident_directory_entries{0};
    std::atomic<size_t> _resident_directory_bytes{0};
};

class TantivyQueryCache {
public:
    using BitmapPtr = std::shared_ptr<const roaring::Roaring>;

    TantivyQueryCache(size_t capacity, size_t max_entry_bytes, size_t max_key_bytes, double admission_threshold,
                      size_t ghost_entries, MemTracker* mem_tracker);
    ~TantivyQueryCache();

    BitmapPtr lookup(const std::string& key);
    void record_bypass();
    void maybe_insert(const std::string& key, const roaring::Roaring& bitmap);
    void erase(const std::string& key);
    void prune();
    void set_capacity(size_t capacity);

    size_t capacity() const;
    size_t memory_usage() const;
    TantivyQueryCacheStats stats() const;

private:
    struct CacheValue;
    using Cache = DynamicCache<std::string, CacheValue, std::mutex>;

    bool _admit(const std::string& key);
    std::unique_ptr<Cache> _cache;
    MemTracker* _mem_tracker;
    size_t _max_entry_bytes;
    size_t _max_key_bytes;
    double _admission_threshold;
    size_t _ghost_entries;

    std::mutex _ghost_mutex;
    std::unordered_map<uint64_t, int64_t> _ghost;

    std::atomic<uint64_t> _lookup_count{0};
    std::atomic<uint64_t> _hit_count{0};
    std::atomic<uint64_t> _miss_count{0};
    std::atomic<uint64_t> _bypass_count{0};
    std::atomic<uint64_t> _insert_count{0};
    std::atomic<uint64_t> _oversize_reject_count{0};
    std::atomic<uint64_t> _key_too_large_count{0};
    std::atomic<uint64_t> _ghost_record_count{0};
    std::atomic<uint64_t> _ghost_admit_count{0};
    std::atomic<size_t> _entries{0};
    std::atomic<size_t> _estimated_resident_bytes{0};
};

class TantivyCacheManager {
public:
    TantivyCacheManager(size_t reader_capacity, size_t reader_max_entries, size_t reader_max_entry_bytes,
                        MemTracker* reader_mem_tracker, size_t query_capacity, size_t query_max_entry_bytes,
                        size_t query_max_key_bytes, double query_admission_threshold, size_t query_ghost_entries,
                        MemTracker* query_mem_tracker);

    TantivyReaderCache* reader_cache() { return &_reader_cache; }
    TantivyQueryCache* query_cache() { return &_query_cache; }
    std::shared_ptr<TantivyReadBufferPool> read_buffer_pool() { return _read_buffer_pool; }

private:
    // Declared before the caches so it is destroyed after cached reader
    // resources. Individual resources also retain a shared reference.
    std::shared_ptr<TantivyReadBufferPool> _read_buffer_pool;
    TantivyReaderCache _reader_cache;
    TantivyQueryCache _query_cache;
};

} // namespace starrocks
