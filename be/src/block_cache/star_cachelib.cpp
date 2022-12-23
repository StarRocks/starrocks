// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "block_cache/star_cachelib.h"

#include "common/logging.h"
#include "common/statusor.h"
#include "util/filesystem_util.h"

namespace starrocks {

Status StarCacheLib::init(const CacheOptions& options) {
    starcache::CacheOptions opt;
    opt.mem_quota_bytes = options.mem_space_size;
    for (auto& dir : options.disk_spaces) {
        opt.disk_dir_spaces.push_back({.path = dir.path, .quota_bytes = dir.size});
    }
    _cache = std::make_unique<starcache::StarCache>();
    return _cache->init(opt);
}

[[maybe_unused]] static void empty_deleter(void* buf) {}

Status StarCacheLib::write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) {
    butil::IOBuf buf;
    buf.append_user_data((void*)value, size, empty_deleter);
    // buf.append(value, size);
    return _cache->set(key, buf);
}

StatusOr<size_t> StarCacheLib::read_cache(const std::string& key, char* value, size_t off, size_t size) {
    butil::IOBuf buf;
    RETURN_IF_ERROR(_cache->read(key, off, size, &buf));
    buf.copy_to(value);
    return buf.size();
}

Status StarCacheLib::remove_cache(const std::string& key) {
    _cache->remove(key);
    return Status::OK();
}

Status StarCacheLib::shutdown() {
    return Status::OK();
}

} // namespace starrocks
