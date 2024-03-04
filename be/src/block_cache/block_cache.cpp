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

BlockCache* BlockCache::instance() {
    static BlockCache cache;
    return &cache;
}

Status BlockCache::init(const CacheOptions& options) {
<<<<<<< HEAD
    for (auto& dir : options.disk_spaces) {
        if (dir.size == 0) {
            continue;
        }
        fs::path dir_path(dir.path);
        if (fs::exists(dir_path)) {
            if (!fs::is_directory(dir_path)) {
                LOG(ERROR) << "the block cache disk path already exists but not a directory, path: " << dir.path;
                return Status::InvalidArgument("invalid block cache disk path");
            }
        } else {
            std::error_code ec;
            if (!fs::create_directory(dir_path, ec)) {
                LOG(ERROR) << "create block cache disk path failed, path: " << dir.path << ", reason: " << ec.message();
                return Status::InvalidArgument("invalid block cache disk path");
            }
        }
    }
    _block_size = options.block_size;
    _kv_cache = std::make_unique<FbCacheLib>();
    return _kv_cache->init(options);
=======
    _block_size = std::min(options.block_size, MAX_BLOCK_SIZE);
#ifdef WITH_CACHELIB
    if (options.engine == "cachelib") {
        _kv_cache = std::make_unique<CacheLibWrapper>();
        LOG(INFO) << "init cachelib engine, block_size: " << _block_size;
    }
#endif
#ifdef WITH_STARCACHE
    if (options.engine == "starcache") {
        _kv_cache = std::make_unique<StarCacheWrapper>();
        LOG(INFO) << "init starcache engine, block_size: " << _block_size;
    }
#endif
    if (!_kv_cache) {
        LOG(ERROR) << "unsupported block cache engine: " << options.engine;
        return Status::NotSupported("unsupported block cache engine");
    }
    RETURN_IF_ERROR(_kv_cache->init(options));
    _initialized.store(true, std::memory_order_relaxed);
    return Status::OK();
>>>>>>> d729b827c2 ([BugFix] Format the datacache path configurations before checking it instead of returning error directly. (#41921))
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
