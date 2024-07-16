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

#include <bvar/bvar.h>

#include <atomic>

#include "block_cache/disk_space_monitor.h"
#include "block_cache/kv_cache.h"
#include "common/status.h"

namespace starrocks {

class BlockCacheHitRateCounter {
public:
    static BlockCacheHitRateCounter* instance();
    ~BlockCacheHitRateCounter() = default;
    void update(uint64_t hit_bytes, uint64_t miss_bytes);
    double hit_rate() const;
    double hit_rate_last_minute() const;
    ssize_t get_hit_bytes() const;
    ssize_t get_miss_bytes() const;
    ssize_t get_hit_bytes_last_minute() const;
    ssize_t get_miss_bytes_last_minute() const;

private:
    static double hit_rate_calculate(ssize_t hit_bytes, ssize_t miss_bytes);
    bvar::Adder<ssize_t> _hit_bytes;
    bvar::Adder<ssize_t> _miss_bytes;
    bvar::Window<bvar::Adder<ssize_t>> _hit_bytes_last_minute{&_hit_bytes, 60};
    bvar::Window<bvar::Adder<ssize_t>> _miss_bytes_last_minute{&_miss_bytes, 60};
};

class BlockCache {
public:
    using CacheKey = std::string;
    using DeleterFunc = std::function<void()>;

    // Return a singleton block cache instance
    static BlockCache* instance();

    ~BlockCache();

    // Init the block cache instance
    Status init(const CacheOptions& options);

    // Write data buffer to cache, the `offset` must be aligned by block size
    Status write_buffer(const CacheKey& cache_key, off_t offset, const IOBuffer& buffer,
                        WriteCacheOptions* options = nullptr);

    Status write_buffer(const CacheKey& cache_key, off_t offset, size_t size, const char* data,
                        WriteCacheOptions* options = nullptr);

    // Write object to cache, the `ptr` is the object pointer.
    Status write_object(const CacheKey& cache_key, const void* ptr, size_t size, DeleterFunc deleter,
                        DataCacheHandle* handle, WriteCacheOptions* options = nullptr);

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned. The offset and size must be aligned by block size.
    Status read_buffer(const CacheKey& cache_key, off_t offset, size_t size, IOBuffer* buffer,
                       ReadCacheOptions* options = nullptr);

    StatusOr<size_t> read_buffer(const CacheKey& cache_key, off_t offset, size_t size, char* data,
                                 ReadCacheOptions* options = nullptr);

    // Read object from cache, the `handle` wraps the object pointer.
    // As long as the handle object is not destroyed and the user does not manully call the `handle->release()`
    // function, the corresponding pointer will never be freed by the cache system.
    Status read_object(const CacheKey& cache_key, DataCacheHandle* handle, ReadCacheOptions* options = nullptr);

    // Remove data from cache. The offset and size must be aligned by block size
    Status remove(const CacheKey& cache_key, off_t offset, size_t size);

    // Update the datacache memory quota.
    Status update_mem_quota(size_t quota_bytes, bool flush_to_disk);

    // Update the datacache disk space infomation, such as disk quota or disk path.
    Status update_disk_spaces(const std::vector<DirSpace>& spaces);

    // Adjust the disk spaces, the space quota will be adjusted based on current disk usage before updating.
    Status adjust_disk_spaces(const std::vector<DirSpace>& spaces);

    void record_read_remote(size_t size, int64_t lateny_us);

    void record_read_cache(size_t size, int64_t lateny_us);

    const DataCacheMetrics cache_metrics(int level = 0) const;

    // Shutdown the cache instance to save some state meta
    Status shutdown();

    size_t block_size() const { return _block_size; }

    bool is_initialized() const { return _initialized.load(std::memory_order_relaxed); }

    bool has_mem_cache() const { return _mem_quota.load(std::memory_order_relaxed) > 0; }

    bool has_disk_cache() const { return _disk_quota.load(std::memory_order_relaxed) > 0; }

    bool available() const { return is_initialized() && (has_mem_cache() || has_disk_cache()); }

    DataCacheEngineType engine_type();

    static const size_t MAX_BLOCK_SIZE;

private:
#ifndef BE_TEST
    BlockCache() = default;
#endif
    void _refresh_quota();

    size_t _block_size = 0;
    std::unique_ptr<KvCache> _kv_cache;
    std::unique_ptr<DiskSpaceMonitor> _disk_space_monitor;
    std::atomic<bool> _initialized = false;
    std::atomic<size_t> _mem_quota = 0;
    std::atomic<size_t> _disk_quota = 0;
};

} // namespace starrocks
