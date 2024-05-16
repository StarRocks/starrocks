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

BlockCache::~BlockCache() {
    (void)shutdown();
}

Status BlockCache::init(const CacheOptions& options) {
    _block_size = std::min(options.block_size, MAX_BLOCK_SIZE);
    auto cache_options = options;
#ifdef WITH_CACHELIB
    if (cache_options.engine == "cachelib") {
        _kv_cache = std::make_unique<CacheLibWrapper>();
        LOG(INFO) << "init cachelib engine, block_size: " << _block_size;
    }
#endif
#ifdef WITH_STARCACHE
    if (cache_options.engine == "starcache") {
        _kv_cache = std::make_unique<StarCacheWrapper>();
        _disk_space_monitor = std::make_unique<DiskSpaceMonitor>(this);
        _disk_space_monitor->adjust_spaces(&cache_options.disk_spaces);
        LOG(INFO) << "init starcache engine, block_size: " << _block_size;
    }
#endif
    if (!_kv_cache) {
        LOG(ERROR) << "unsupported block cache engine: " << cache_options.engine;
        return Status::NotSupported("unsupported block cache engine");
    }
    RETURN_IF_ERROR(_kv_cache->init(cache_options));
    _initialized.store(true, std::memory_order_relaxed);
    if (_disk_space_monitor) {
        _disk_space_monitor->start();
    }
    return Status::OK();
}

Status BlockCache::write_buffer(const CacheKey& cache_key, off_t offset, const IOBuffer& buffer,
                                WriteCacheOptions* options) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "write block key: " << cache_key << " with invalid args, offset: " << offset;
        return Status::InvalidArgument(strings::Substitute("offset must be aligned by block size $0", _block_size));
    }
    if (buffer.empty()) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->write_buffer(block_key, buffer, options);
}

static void empty_deleter(void*) {}

Status BlockCache::write_buffer(const CacheKey& cache_key, off_t offset, size_t size, const char* data,
                                WriteCacheOptions* options) {
    if (!data) {
        return Status::InvalidArgument("invalid data buffer");
    }

    IOBuffer buffer;
    buffer.append_user_data((void*)data, size, empty_deleter);
    return write_buffer(cache_key, offset, buffer, options);
}

Status BlockCache::write_object(const CacheKey& cache_key, const void* ptr, size_t size, DeleterFunc deleter,
                                DataCacheHandle* handle, WriteCacheOptions* options) {
    if (!ptr) {
        return Status::InvalidArgument("invalid object pointer");
    }
    return _kv_cache->write_object(cache_key, ptr, size, std::move(deleter), handle, options);
}

Status BlockCache::read_buffer(const CacheKey& cache_key, off_t offset, size_t size, IOBuffer* buffer,
                               ReadCacheOptions* options) {
    if (size == 0) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->read_buffer(block_key, offset - index * _block_size, size, buffer, options);
}

StatusOr<size_t> BlockCache::read_buffer(const CacheKey& cache_key, off_t offset, size_t size, char* data,
                                         ReadCacheOptions* options) {
    IOBuffer buffer;
    RETURN_IF_ERROR(read_buffer(cache_key, offset, size, &buffer, options));
    buffer.copy_to(data);
    return buffer.size();
}

Status BlockCache::read_object(const CacheKey& cache_key, DataCacheHandle* handle, ReadCacheOptions* options) {
    return _kv_cache->read_object(cache_key, handle, options);
}

Status BlockCache::remove(const CacheKey& cache_key, off_t offset, size_t size) {
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
    return _kv_cache->remove(block_key);
}

Status BlockCache::update_mem_quota(size_t quota_bytes) {
    return _kv_cache->update_mem_quota(quota_bytes);
}

Status BlockCache::update_disk_spaces(const std::vector<DirSpace>& spaces) {
    return _kv_cache->update_disk_spaces(spaces);
}

Status BlockCache::adjust_disk_spaces(const std::vector<DirSpace>& spaces) {
    if (_disk_space_monitor) {
        auto adjusted_spaces = spaces;
        _disk_space_monitor->adjust_spaces(&adjusted_spaces);
        return _disk_space_monitor->adjust_cache_quota(adjusted_spaces);
    }
    return update_disk_spaces(spaces);
}

void BlockCache::record_read_remote(size_t size, int64_t lateny_us) {
    _kv_cache->record_read_remote(size, lateny_us);
}

void BlockCache::record_read_cache(size_t size, int64_t lateny_us) {
    _kv_cache->record_read_cache(size, lateny_us);
}

const DataCacheMetrics BlockCache::cache_metrics(int level) const {
    return _kv_cache->cache_metrics(level);
}

Status BlockCache::shutdown() {
    if (!_initialized.load(std::memory_order_relaxed)) {
        return Status::OK();
    }
    Status st = _kv_cache->shutdown();
    if (_disk_space_monitor) {
        _disk_space_monitor->stop();
    }
    _initialized.store(false, std::memory_order_relaxed);
    return st;
}

DataCacheEngineType BlockCache::engine_type() {
    return _kv_cache->engine_type();
}

} // namespace starrocks
