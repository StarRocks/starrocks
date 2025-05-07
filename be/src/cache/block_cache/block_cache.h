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

namespace starrocks {

class BlockCache {
public:
    using CacheKey = std::string;

    // Return a singleton block cache instance
    static BlockCache* instance();

    BlockCache() = default;
    ~BlockCache();

    // Init the block cache instance
    Status init(const CacheOptions& options, std::shared_ptr<LocalCache> local_cache,
                std::shared_ptr<RemoteCache> remote_cache);

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

    // Read data from remote cache
    Status read_buffer_from_remote_cache(const std::string& cache_key, size_t offset, size_t size, IOBuffer* buffer,
                                         ReadCacheOptions* options);

    void record_read_local_cache(size_t size, int64_t latency_us);

    void record_read_remote_cache(size_t size, int64_t latency_us);

    void record_read_remote_storage(size_t size, int64_t latency_us, bool local_only);

    // Shutdown the cache instance to save some state meta
    Status shutdown();

    size_t block_size() const { return _block_size; }

    bool is_initialized() const { return _initialized.load(std::memory_order_relaxed); }

    bool available() const { return is_initialized() && _local_cache->available(); }
    bool mem_cache_available() const { return is_initialized() && _local_cache->mem_cache_available(); }
    LocalCache* local_cache() const { return _local_cache.get(); }
    std::shared_ptr<LocalCache> local_cache_ptr() { return _local_cache; }

    static const size_t MAX_BLOCK_SIZE;

private:
    size_t _block_size = 0;
    std::shared_ptr<LocalCache> _local_cache;
    std::shared_ptr<RemoteCache> _remote_cache;
    std::atomic<bool> _initialized = false;
};

} // namespace starrocks
