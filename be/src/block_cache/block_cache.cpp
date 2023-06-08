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

#include "block_cache/block_cache.h"

#include <fmt/format.h>

#include <filesystem>

#ifdef WITH_CACHELIB
#include "block_cache/cachelib_wrapper.h"
#endif
#ifdef WITH_STARCACHE
#include "block_cache/starcache_wrapper.h"
#endif
#include "common/config.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

namespace fs = std::filesystem;

// The cachelib doesn't support a item (key+valueu+attribute) larger than 4 MB without chain.
// So, we check and limit the block_size configured by users to avoid unexpected errors.
// For starcache, in theory we doesn't have a hard limitation for block size, but a very large
// block_size may cause heavy read amplification. So, we also limit it to 2 MB as an empirical value.
const size_t BlockCache::MAX_BLOCK_SIZE = 2 * 1024 * 1024;

BlockCache* BlockCache::instance() {
    static BlockCache cache;
    return &cache;
}

Status BlockCache::init(const CacheOptions& options) {
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
        LOG(INFO) << "init starcache block engine";
    }
#endif
    if (!_kv_cache) {
        LOG(ERROR) << "unsupported block cache engine: " << options.engine;
        return Status::NotSupported("unsupported block cache engine");
    }
    return _kv_cache->init(options);
}

Status BlockCache::write_cache(const CacheKey& cache_key, off_t offset, size_t size, const char* buffer,
                               size_t ttl_seconds, bool overwrite) {
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
    return _kv_cache->write_cache(block_key, buffer, size, ttl_seconds, overwrite);
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
    Status st = _kv_cache->shutdown();
    _kv_cache = nullptr;
    return st;
}

} // namespace starrocks
