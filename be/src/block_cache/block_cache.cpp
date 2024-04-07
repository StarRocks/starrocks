// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "block_cache/block_cache.h"

#include <fmt/format.h>

#include <filesystem>

#ifdef WITH_CACHELIB
#include "block_cache/cachelib_wrapper.h"
#endif
#include "block_cache/fb_cachelib.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

namespace fs = std::filesystem;

// The cachelib doesn't support a item (key+valueu+attribute) larger than 4 MB without chain.
// So, we check and limit the block_size configured by users to avoid unexpected errors.
const size_t BlockCache::MAX_BLOCK_SIZE = 2 * 1024 * 1024;

BlockCache* BlockCache::instance() {
    static BlockCache cache;
    return &cache;
}

Status BlockCache::init(const CacheOptions& options) {
    _block_size = std::min(options.block_size, MAX_BLOCK_SIZE);
    _kv_cache = std::make_unique<FbCacheLib>();
    return _kv_cache->init(options);
}

Status BlockCache::write_cache(const CacheKey& cache_key, off_t offset, size_t size, const char* buffer,
                               size_t ttl_seconds) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "write block key: " << cache_key << " with invalid args, offset: " << offset;
        return Status::InvalidArgument(strings::Substitute("offset must be aligned by block size $0", _block_size));
    }
    if (!buffer) {
        return Status::InvalidArgument("invalid data buffer");
    }
    if (size == 0) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->write_cache(block_key, buffer, size, ttl_seconds);
}

StatusOr<size_t> BlockCache::read_cache(const CacheKey& cache_key, off_t offset, size_t size, char* buffer) {
    // when buffer == nullptr, it can check if cached.
    if (size == 0) {
        return 0;
    }
    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->read_cache(block_key, buffer, offset - index * _block_size, size);
}

Status BlockCache::remove_cache(const CacheKey& cache_key, off_t offset, size_t size) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "remove block key: " << cache_key << " with invalid args, offset: " << offset
                     << ", size: " << size;
        return Status::InvalidArgument(
                strings::Substitute("offset and size must be aligned by block size $0", _block_size));
    }
    if (size == 0) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->remove_cache(block_key);
}

Status BlockCache::shutdown() {
    return _kv_cache->shutdown();
}

} // namespace starrocks
