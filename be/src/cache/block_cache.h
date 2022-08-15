// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "common/status.h"

#pragma once

namespace starrocks {

typedef std::string CacheKey;

class BlockCache {
public:
    ~BlockCache() {}

    BlockCache(const BlockCache&)=delete;
    BlockCache& operator=(const BlockCache&)=delete;

    // Return a singleton block cache instance
    static BlockCache& get_instance();

    // Set block size as a logical cache unit
    Status set_block_size(size_t block_size);

    // Set memory space size for memory tier
    Status set_mem_space(size_t mem_size);

    // Set disk space for disk tier, and it can consist of several different directories 
    Status set_disk_space(const std::vector<std::string>& dirs, const std::vector<size_t>& sizes);

    // Write data to cache, the offset must be aligned by block size 
    Status write_cache(const CacheKey& cache_key, off_t offset, size_t size, const void* buffer,
                       size_t ttl_seconds = 0);

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned. The offset must be aligned by block size.
    StatusOr<size_t> read_cache(const CacheKey& cache_key, off_t offset, size_t size, void* buffer);

    // Remove data from cache. The offset must be aligned by block size 
    Status remove_cache(const CacheKey& cache_key, off_t offset, size_t size);

private:
    BlockCache() {}

    size_t _block_size = 0;
};

} // namespace starrocks
