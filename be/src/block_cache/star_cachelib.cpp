// Copyright 2023-present StarRocks, Inc. All rights reserved.
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

#include "block_cache/star_cachelib.h"

#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/fastmem.h"
#include "util/filesystem_util.h"

namespace starrocks {

Status StarCacheLib::init(const CacheOptions& options) {
    starcache::CacheOptions opt;
    opt.mem_quota_bytes = options.mem_space_size;
    for (auto& dir : options.disk_spaces) {
        opt.disk_dir_spaces.push_back({.path = dir.path, .quota_bytes = dir.size});
    }
    opt.checksum = options.checksum;
    opt.block_size = options.block_size;
    _cache = std::make_unique<starcache::StarCache>();
    return to_status(_cache->init(opt));
}

static void empty_deleter(void* buf) {}

Status StarCacheLib::write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) {
    butil::IOBuf buf;
    // Don't free the buffer passed by users
    buf.append_user_data((void*)value, size, empty_deleter);
    return to_status(_cache->set(key, buf));
}

StatusOr<size_t> StarCacheLib::read_cache(const std::string& key, char* value, size_t off, size_t size) {
    butil::IOBuf buf;
    RETURN_IF_ERROR(to_status(_cache->read(key, off, size, &buf)));
    // to check if cached.
    if (value == nullptr) {
        return 0;
    }
    copy_iobuf(buf, value);
    return buf.size();
}

Status StarCacheLib::remove_cache(const std::string& key) {
    _cache->remove(key);
    return Status::OK();
}

std::unordered_map<std::string, double> StarCacheLib::cache_stats() {
    // TODO: fill some statistics information
    std::unordered_map<std::string, double> stats;
    return stats;
}

Status StarCacheLib::shutdown() {
    return Status::OK();
}

void copy_iobuf(const butil::IOBuf& buf, char* value) {
    off_t off = 0;
    for (size_t i = 0; i < buf.backing_block_num(); ++i) {
        auto sp = buf.backing_block(i);
        if (!sp.empty()) {
            strings::memcpy_inlined(value + off, (void*)sp.data(), sp.size());
            off += sp.size();
        }
    }
}

} // namespace starrocks
