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
    opt.enable_disk_checksum = options.checksum;
    opt.max_concurrent_writes = options.max_concurrent_inserts;

    _cache = std::make_unique<starcache::StarCache>();
    return to_status(_cache->init(opt));
}

Status StarCacheWrapper::write_cache(const std::string& key, const IOBuffer& buffer, size_t ttl_seconds,
                                     bool overwrite) {
    starcache::WriteOptions options;
    options.ttl_seconds = ttl_seconds;
    options.overwrite = overwrite;
    return to_status(_cache->set(key, buffer.const_raw_buf(), &options));

}

Status StarCacheWrapper::read_cache(const std::string& key, size_t off, size_t size, IOBuffer* buffer) {
    return to_status(_cache->read(key, off, size, &buffer->raw_buf()));
}

Status StarCacheWrapper::remove_cache(const std::string& key) {
    _cache->remove(key);
    return Status::OK();
}

std::unordered_map<std::string, double> StarCacheWrapper::cache_stats() {
    // TODO: fill some statistics information
    std::unordered_map<std::string, double> stats;
    return stats;
}

Status StarCacheWrapper::shutdown() {
    return Status::OK();
}

} // namespace starrocks
