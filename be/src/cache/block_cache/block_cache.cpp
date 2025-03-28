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

#include "cache/block_cache/block_cache.h"

#include <fmt/format.h>

#include "runtime/exec_env.h"

#ifdef WITH_STARCACHE
#include "cache/block_cache/starcache_wrapper.h"
#endif
#include "common/statusor.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

namespace fs = std::filesystem;

// For starcache, in theory we doesn't have a hard limitation for block size, but a very large
// block_size may cause heavy read amplification. So, we also limit it to 2 MB as an empirical value.
const size_t BlockCache::MAX_BLOCK_SIZE = 2 * 1024 * 1024;

BlockCache* BlockCache::instance() {
    return CacheEnv::GetInstance()->block_cache();
}

BlockCache::~BlockCache() {
    (void)shutdown();
}

Status BlockCache::init(const CacheOptions& options) {
    _block_size = std::min(options.block_size, MAX_BLOCK_SIZE);
    auto cache_options = options;
#ifdef WITH_STARCACHE
    if (cache_options.engine == "starcache") {
        _kv_cache = std::make_unique<StarCacheWrapper>();
        _disk_space_monitor = std::make_unique<DiskSpaceMonitor>(this);
        RETURN_IF_ERROR(_disk_space_monitor->init(&cache_options.disk_spaces));
        LOG(INFO) << "init starcache engine, block_size: " << _block_size
                  << ", disk_spaces: " << _disk_space_monitor->to_string(cache_options.disk_spaces);
    }
#endif
    if (!_kv_cache) {
        LOG(ERROR) << "unsupported block cache engine: " << cache_options.engine;
        return Status::NotSupported("unsupported block cache engine");
    }
    RETURN_IF_ERROR(_kv_cache->init(cache_options));
    _refresh_quota();
    _initialized.store(true, std::memory_order_relaxed);
    if (_disk_space_monitor) {
        _disk_space_monitor->start();
    }
    return Status::OK();
}

Status BlockCache::write(const CacheKey& cache_key, off_t offset, const IOBuffer& buffer, WriteCacheOptions* options) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "write block key: " << cache_key << " with invalid args, offset: " << offset;
        return Status::InvalidArgument(strings::Substitute("offset must be aligned by block size $0", _block_size));
    }
    if (buffer.empty()) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->write(block_key, buffer, options);
}

static void empty_deleter(void*) {}

Status BlockCache::write(const CacheKey& cache_key, off_t offset, size_t size, const char* data,
                         WriteCacheOptions* options) {
    if (!data) {
        return Status::InvalidArgument("invalid data buffer");
    }

    IOBuffer buffer;
    buffer.append_user_data((void*)data, size, empty_deleter);
    return write(cache_key, offset, buffer, options);
}

Status BlockCache::read(const CacheKey& cache_key, off_t offset, size_t size, IOBuffer* buffer,
                        ReadCacheOptions* options) {
    if (size == 0) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->read(block_key, offset - index * _block_size, size, buffer, options);
}

StatusOr<size_t> BlockCache::read(const CacheKey& cache_key, off_t offset, size_t size, char* data,
                                  ReadCacheOptions* options) {
    IOBuffer buffer;
    RETURN_IF_ERROR(read(cache_key, offset, size, &buffer, options));
    buffer.copy_to(data);
    return buffer.size();
}

bool BlockCache::exist(const CacheKey& cache_key, off_t offset, size_t size) const {
    if (size == 0) {
        return true;
    }
    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->exist(block_key);
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

Status BlockCache::update_mem_quota(size_t quota_bytes, bool flush_to_disk) {
    Status st = _kv_cache->update_mem_quota(quota_bytes, flush_to_disk);
    _refresh_quota();
    return st;
}

Status BlockCache::update_disk_spaces(const std::vector<DirSpace>& spaces) {
    Status st = _kv_cache->update_disk_spaces(spaces);
    _refresh_quota();
    return st;
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
    _kv_cache.reset();
    return st;
}

void BlockCache::disk_spaces(std::vector<DirSpace>* spaces) {
    spaces->clear();
    auto metrics = _kv_cache->cache_metrics(0);
    for (auto& dir : metrics.disk_dir_spaces) {
        spaces->push_back({.path = dir.path, .size = dir.quota_bytes});
    }
}

DataCacheEngineType BlockCache::engine_type() {
    return _kv_cache->engine_type();
}

void BlockCache::_refresh_quota() {
    auto metrics = _kv_cache->cache_metrics(0);
    _mem_quota.store(metrics.mem_quota_bytes, std::memory_order_relaxed);
    _disk_quota.store(metrics.disk_quota_bytes, std::memory_order_relaxed);
}

} // namespace starrocks
