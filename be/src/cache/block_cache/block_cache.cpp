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

#include "cache/block_cache/peer_cache_wrapper.h"
#include "common/statusor.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "util/hash_util.hpp"

namespace starrocks {

namespace fs = std::filesystem;

// For starcache, in theory we don't have a hard limitation for block size, but a very large
// block_size may cause heavy read amplification. So, we also limit it to 2 MB as an empirical value.
const size_t BlockCache::MAX_BLOCK_SIZE = 2 * 1024 * 1024;

BlockCache* BlockCache::instance() {
    return CacheEnv::GetInstance()->block_cache();
}

BlockCache::~BlockCache() {
    (void)shutdown();
}

Status BlockCache::init(const CacheOptions& options, std::shared_ptr<LocalCache> local_cache,
                        std::shared_ptr<RemoteCache> remote_cache) {
    _block_size = std::min(options.block_size, MAX_BLOCK_SIZE);
    _local_cache = std::move(local_cache);
    _remote_cache = std::move(remote_cache);

    _initialized.store(true, std::memory_order_relaxed);
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
    return _local_cache->write(block_key, buffer, options);
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
    return _local_cache->read(block_key, offset - index * _block_size, size, buffer, options);
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
    return _local_cache->exist(block_key);
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
    return _local_cache->remove(block_key);
}

Status BlockCache::read_buffer_from_remote_cache(const std::string& cache_key, size_t offset, size_t size,
                                                 IOBuffer* buffer, ReadCacheOptions* options) {
    if (size == 0) {
        return Status::OK();
    }

    return _remote_cache->read(cache_key, offset, size, buffer, options);
}

void BlockCache::record_read_remote_storage(size_t size, int64_t latency_us, bool local_only) {
    _local_cache->record_read_remote(size, latency_us);
    if (!local_only) {
        _remote_cache->record_read_remote(size, latency_us);
    }
}

void BlockCache::record_read_local_cache(size_t size, int64_t latency_us) {
    _local_cache->record_read_cache(size, latency_us);
}

void BlockCache::record_read_remote_cache(size_t size, int64_t latency_us) {
    _remote_cache->record_read_cache(size, latency_us);
}

Status BlockCache::shutdown() {
    if (!_initialized.load(std::memory_order_relaxed)) {
        return Status::OK();
    }
    _initialized.store(false, std::memory_order_relaxed);

    _local_cache.reset();
    _remote_cache.reset();

    return Status::OK();
}

} // namespace starrocks
