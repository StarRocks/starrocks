// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "block_cache/block_cache.h"

#include <fmt/format.h>

#include "block_cache/fb_cachelib.h"
#include "block_cache/star_cachelib.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

BlockCache::BlockCache() {
}

BlockCache* BlockCache::instance() {
    static BlockCache cache;
    return &cache;
}

Status BlockCache::init(const CacheOptions& options) {
    // TODO: check block size limit
    _block_size = options.block_size;
    if (options.engine == "cachelib") {
        _kv_cache = std::make_unique<FbCacheLib>();
        LOG(INFO) << "init cachelib block engine";
    } else if (options.engine == "starcache") {
        _kv_cache = std::make_unique<StarCacheLib>();
        LOG(INFO) << "init starcache block engine";
    } else {
        LOG(ERROR) << "unsupported block cache engine: " << options.engine;
        return Status::NotSupported("unsupported block cache engine");
    }
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

    size_t start_block_index = offset / _block_size;
    size_t end_block_index = (offset + size - 1) / _block_size + 1;
    off_t off_in_buf = 0;
    for (size_t index = start_block_index; index < end_block_index; ++index) {
        std::string block_key = fmt::format("{}/{}", cache_key, index);
        const char* block_buf = buffer + off_in_buf;
        const size_t block_size = std::min(size - off_in_buf, _block_size);
        RETURN_IF_ERROR(_kv_cache->write_cache(block_key, block_buf, block_size, ttl_seconds));
        off_in_buf += block_size;
    }

    return Status::OK();
}

StatusOr<size_t> BlockCache::read_cache(const CacheKey& cache_key, off_t offset, size_t size, char* buffer) {
    if (!buffer) {
        return Status::InvalidArgument("invalid data buffer");
    }
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

    size_t start_block_index = offset / _block_size;
    size_t end_block_index = (offset + size - 1) / _block_size + 1;
    for (size_t index = start_block_index; index < end_block_index; ++index) {
        std::string block_key = fmt::format("{}/{}", cache_key, index);
        RETURN_IF_ERROR(_kv_cache->remove_cache(block_key));
    }
    return Status::OK();
}

Status BlockCache::shutdown() {
    return _kv_cache->shutdown();
}

} // namespace starrocks
