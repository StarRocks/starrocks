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

#include <atomic>

#include "cache/block_cache/disk_space_monitor.h"
#include "cache/block_cache/local_cache.h"
#include "cache/block_cache/remote_cache.h"
#include "common/status.h"
#include "cache/block_cache/block_cache_metrics.h"

namespace starrocks {

class BlockCache {
public:
    using CacheKey = std::string;
    using DeleterFunc = std::function<void()>;

    // Return a singleton block cache instance
    static BlockCache* instance();

    BlockCache() = default;
    ~BlockCache();

    // Init the block cache instance
    Status init(const CacheOptions& options);

    // Write data buffer to cache, the `offset` must be aligned by block size
    Status write(const CacheKey& cache_key, off_t offset, const IOBuffer& buffer, WriteCacheOptions* options = nullptr);

    Status write(const CacheKey& cache_key, off_t offset, size_t size, const char* data,
                 WriteCacheOptions* options = nullptr);

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned. The offset and size must be aligned by block size.
    Status read(const CacheKey& cache_key, off_t offset, size_t size, IOBuffer* buffer,
                ReadCacheOptions* options = nullptr);

    StatusOr<size_t> read(const CacheKey& cache_key, off_t offset, size_t size, char* data,
                          ReadCacheOptions* options = nullptr);

    bool exist(const CacheKey& cache_key, off_t offset, size_t size) const;

    // Remove data from cache. The offset and size must be aligned by block size
    Status remove(const CacheKey& cache_key, off_t offset, size_t size);

    // Update the datacache memory quota.
    Status update_mem_quota(size_t quota_bytes, bool flush_to_disk);

    // Update the datacache disk space infomation, such as disk quota or disk path.
    Status update_disk_spaces(const std::vector<DirSpace>& spaces);

    // Get datacache metrics.
    // The level can be: 0, 1, 2. The higher the level, more detailed metrics will be returned.
    const DataCacheMetrics cache_metrics(int level = 0) const;

    // Read data from remote cache
    Status read_buffer_from_remote_cache(const std::string& cache_key, size_t offset, size_t size, IOBuffer* buffer,
                                         ReadCacheOptions* options);

    void record_read_local_cache(size_t size, int64_t lateny_us);

    void record_read_remote_cache(size_t size, int64_t lateny_us);

    void record_read_remote_storage(size_t size, int64_t lateny_us, bool local_only);

    // Shutdown the cache instance to save some state meta
    Status shutdown();

    size_t block_size() const { return _block_size; }

    bool is_initialized() const { return _initialized.load(std::memory_order_relaxed); }

    bool has_mem_cache() const { return _mem_quota.load(std::memory_order_relaxed) > 0; }

    bool has_disk_cache() const { return _disk_quota.load(std::memory_order_relaxed) > 0; }

    bool available() const { return is_initialized() && (has_mem_cache() || has_disk_cache()); }

    void disk_spaces(std::vector<DirSpace>* spaces);

    DataCacheEngineType engine_type();

#ifdef WITH_STARCACHE
    std::shared_ptr<starcache::StarCache> starcache_instance() { return _local_cache->starcache_instance(); }
#endif

    static const size_t MAX_BLOCK_SIZE;

private:
    void _refresh_quota();
    void _update_metrics();

    size_t _block_size = 0;
    std::shared_ptr<LocalCache> _local_cache;
    std::shared_ptr<RemoteCache> _remote_cache;
    std::unique_ptr<DiskSpaceMonitor> _disk_space_monitor;
    std::vector<std::string> _disk_paths;
    std::atomic<bool> _initialized = false;
    std::atomic<size_t> _mem_quota = 0;
    std::atomic<size_t> _disk_quota = 0;
    BlockCacheMetrics _metrics;
};

} // namespace starrocks
