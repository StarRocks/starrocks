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

static void empty_deleter(void* buf) {}

Status StarCacheWrapper::init(const CacheOptions& options) {
    starcache::CacheOptions opt;
    opt.mem_quota_bytes = options.mem_space_size;
    for (auto& dir : options.disk_spaces) {
        opt.disk_dir_spaces.push_back({.path = dir.path, .quota_bytes = dir.size});
    }
    _load_starcache_conf();
    starcache::config::FLAGS_block_size = options.block_size;
    starcache::config::FLAGS_enable_disk_checksum = options.enable_checksum;
    starcache::config::FLAGS_max_concurrent_writes = options.max_concurrent_inserts;
    starcache::config::FLAGS_enable_os_page_cache = !options.enable_direct_io;

    _cache = std::make_unique<starcache::StarCache>();
    return to_status(_cache->init(opt));
}

Status StarCacheWrapper::write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds,
                                     bool overwrite) {
    butil::IOBuf buf;
    // Don't free the buffer passed by users
    buf.append_user_data((void*)value, size, empty_deleter);

    starcache::WriteOptions options;
    options.ttl_seconds = ttl_seconds;
    options.overwrite = overwrite;
    return to_status(_cache->set(key, buf, &options));
}

StatusOr<size_t> StarCacheWrapper::read_cache(const std::string& key, char* value, size_t off, size_t size) {
    butil::IOBuf buf;
    RETURN_IF_ERROR(to_status(_cache->read(key, off, size, &buf)));
    // to check if cached.
    if (value == nullptr) {
        return 0;
    }
    copy_iobuf(buf, value);
    return buf.size();
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

void StarCacheWrapper::_load_starcache_conf() {
    const char* starrocks_home = getenv("STARROCKS_HOME");
    const std::string starcache_conf = "conf/starcache.conf";
    std::string conf_file;
    if (starrocks_home) {
        conf_file = std::string(starrocks_home) + "/" + starcache_conf;
    } else {
        conf_file = starcache_conf;
    }
    std::filesystem::path conf_path(conf_file);
    if (std::filesystem::exists(conf_path)) {
        gflags::ReadFromFlagsFile(conf_path.string(), "starcache", true);
        LOG(INFO) << "load gflag configuration file from " << conf_path.string();
    }
}

} // namespace starrocks
