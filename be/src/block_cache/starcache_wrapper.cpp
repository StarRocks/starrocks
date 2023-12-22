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

#include "block_cache/starcache_wrapper.h"

#include <filesystem>

#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/fastmem.h"
#include "util/filesystem_util.h"

namespace starrocks {

Status StarCacheWrapper::init(const CacheOptions& options) {
    starcache::CacheOptions opt;
    opt.mem_quota_bytes = options.mem_space_size;
    for (auto& dir : options.disk_spaces) {
        opt.disk_dir_spaces.push_back({.path = dir.path, .quota_bytes = dir.size});
    }
    opt.block_size = options.block_size;
    opt.enable_disk_checksum = options.enable_checksum;
    opt.max_concurrent_writes = options.max_concurrent_inserts;
    opt.enable_os_page_cache = !options.enable_direct_io;
    if (options.enable_cache_adaptor) {
        _cache_adaptor.reset(starcache::create_default_adaptor(options.skip_read_factor));
        opt.cache_adaptor = _cache_adaptor.get();
    }
    _cache = std::make_unique<starcache::StarCache>();
    return to_status(_cache->init(opt));
}

Status StarCacheWrapper::write_buffer(const std::string& key, const IOBuffer& buffer, WriteCacheOptions* options) {
    if (!options) {
        return to_status(_cache->set(key, buffer.const_raw_buf(), nullptr));
    }
    starcache::WriteOptions opts;
    opts.ttl_seconds = options->ttl_seconds;
    opts.overwrite = options->overwrite;
    auto st = to_status(_cache->set(key, buffer.const_raw_buf(), &opts));
    if (st.ok()) {
        options->stats.write_mem_bytes = opts.stats.write_mem_bytes;
        options->stats.write_disk_bytes = opts.stats.write_disk_bytes;
    }
    return st;
}

Status StarCacheWrapper::write_object(const std::string& key, const void* ptr, size_t size,
                                      std::function<void()> deleter, CacheHandle* handle, WriteCacheOptions* options) {
    if (!options) {
        return to_status(_cache->set_object(key, ptr, size, deleter, handle, nullptr));
    }
    starcache::WriteOptions opts;
    opts.ttl_seconds = options->ttl_seconds;
    opts.overwrite = options->overwrite;
    auto st = to_status(_cache->set_object(key, ptr, size, deleter, handle, &opts));
    if (st.ok()) {
        options->stats.write_mem_bytes = size;
    }
    return st;
}

Status StarCacheWrapper::read_buffer(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                                     ReadCacheOptions* options) {
    if (!options) {
        return to_status(_cache->read(key, off, size, &buffer->raw_buf(), nullptr));
    }
    starcache::ReadOptions opts;
    auto st = to_status(_cache->read(key, off, size, &buffer->raw_buf(), &opts));
    if (st.ok()) {
        options->stats.read_mem_bytes = opts.stats.read_mem_bytes;
        options->stats.read_disk_bytes = opts.stats.read_disk_bytes;
    }
    return st;
}

Status StarCacheWrapper::read_object(const std::string& key, CacheHandle* handle, ReadCacheOptions* options) {
    if (!options) {
        return to_status(_cache->get_object(key, handle, nullptr));
    }
    starcache::ReadOptions opts;
    auto st = to_status(_cache->get_object(key, handle, &opts));
    if (st.ok()) {
        options->stats.read_mem_bytes = opts.stats.read_mem_bytes;
    }
    return st;
}

Status StarCacheWrapper::remove(const std::string& key) {
    _cache->remove(key);
    return Status::OK();
}

const DataCacheMetrics StarCacheWrapper::cache_metrics() {
    return _cache->metrics();
}

void StarCacheWrapper::record_read_remote(size_t size, int64_t lateny_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_remote(size, lateny_us);
    }
}

void StarCacheWrapper::record_read_cache(size_t size, int64_t lateny_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_cache(size, lateny_us);
    }
}

Status StarCacheWrapper::shutdown() {
    return Status::OK();
}

} // namespace starrocks
