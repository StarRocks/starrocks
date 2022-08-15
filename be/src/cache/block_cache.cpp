// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "cache/block_cache.h"

#include "common/config.h"
#include "common/statusor.h"

namespace starrocks {

BlockCache& BlockCache::get_instance() {
    static BlockCache cache;
    return  cache;
}

Status BlockCache::set_block_size(size_t block_size) {
    // TODO: check block size limit
    _block_size = block_size;
    return Status::OK();
}

Status BlockCache::set_mem_space(size_t mem_size) {
    // 
    return Status::OK();
}

Status BlockCache::set_disk_space(const std::vector<std::string>& dirs, const std::vector<size_t>& sizes) {
    //
    return Status::OK();
}

Status BlockCache::write_cache(const CacheKey& cache_key, off_t offset, size_t size, const void* buffer,
                               size_t ttl_seconds) {
    //
    return Status::OK();
}

StatusOr<size_t> BlockCache::read_cache(const CacheKey& cache_key, off_t offset, size_t size, void* buffer) {
    //
    return (size_t)0;
}

Status BlockCache::remove_cache(const CacheKey& cache_key, off_t offset, size_t size) {
    //
    return Status::OK();
}

} // namespace starrocks
