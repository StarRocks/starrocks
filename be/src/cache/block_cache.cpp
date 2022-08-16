// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "cache/block_cache.h"

#include "common/config.h"
#include "common/statusor.h"
#include "storage/options.h"

namespace starrocks {

BlockCache* BlockCache::instance() {
    static BlockCache cache;
    return &cache;
}

void BlockCache::init() {
    BlockCache& cache = *this;
    cache.set_block_size(config::block_cache_block_size);
    cache.set_mem_space(config::block_cache_mem_size);
    std::vector<StorePath> paths;
    parse_conf_store_paths(config::storage_root_path, &paths);
    std::vector<std::string> dirs;
    std::vector<size_t> sizes;
    for (const StorePath& p : paths) {
        dirs.emplace_back(p.path);
        sizes.emplace_back(config::block_cache_disk_size);
    }
    cache.set_disk_space(dirs, sizes);
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
    // return (size_t)0;
    return Status::NotFound("");
}

Status BlockCache::remove_cache(const CacheKey& cache_key, off_t offset, size_t size) {
    //
    return Status::OK();
}

} // namespace starrocks
