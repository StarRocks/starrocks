// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "block_cache/kv_cache.h"
#include "common/status.h"

namespace starrocks {

class BlockCache {
public:
    typedef std::string CacheKey;

    // Return a singleton block cache instance
    static BlockCache* instance();

    // Init the block cache instance
    Status init(const CacheOptions& options);

    // Write data to cache, the offset must be aligned by block size
    Status write_cache(const CacheKey& cache_key, off_t offset, size_t size, const char* buffer,
                       size_t ttl_seconds = 0);

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned. The offset and size must be aligned by block size.
    StatusOr<size_t> read_cache(const CacheKey& cache_key, off_t offset, size_t size, char* buffer);

    // NOTICE: This function is not safe now, as the returned buffer may be evicted before visited
    // by users. We need to implement it more safe by cachelib item handle.
    Status read_cache_zero_copy(const CacheKey& cache_key, off_t offset, size_t size, const char** buf);

    // Remove data from cache. The offset and size must be aligned by block size
    Status remove_cache(const CacheKey& cache_key, off_t offset, size_t size);

    // Shutdown the cache instance to save some state meta
    Status shutdown();

    size_t block_size() const { return _block_size; }

private:
#ifndef BE_TEST
    BlockCache() = default;
#endif

    size_t _block_size = 0;
    std::unique_ptr<KvCache> _kv_cache;
};

} // namespace starrocks
