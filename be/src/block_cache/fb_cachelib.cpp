// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "block_cache/fb_cachelib.h"

#include "common/logging.h"
#include "common/statusor.h"

namespace starrocks {

Status FbCacheLib::init(const CacheOptions& options) {
    Cache::Config config;
    config.setCacheSize(options.mem_space_size).setCacheName("default cache").setAccessConfig({25, 10}).validate();

    Cache::NvmCacheConfig nvmConfig;
    nvmConfig.navyConfig.setBlockSize(4096);

    std::vector<std::string> files;
    for (auto& dir : options.disk_spaces) {
        files.emplace_back(dir.path + "/cachelib_data");
    }
    if (files.empty()) {
        return Status::InvalidArgument("disk paths for block cache can't be empty");
    } else if (files.size() == 1) {
        nvmConfig.navyConfig.setSimpleFile(files[0], options.disk_spaces[0].size, false);
    } else {
        nvmConfig.navyConfig.setRaidFiles(files, options.disk_spaces[0].size, false);
    }
    nvmConfig.navyConfig.blockCache().setRegionSize(16 * 1024 * 1024);
    config.enableNvmCache(nvmConfig);

    _cache = std::make_unique<Cache>(config);
    _default_pool = _cache->addPool("default pool", _cache->getCacheMemoryStats().cacheSize);

    return Status::OK();
}

Status FbCacheLib::write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) {
    // TODO: check size for chain item
    auto handle = _cache->allocate(_default_pool, key, size);
    if (!handle) {
        return Status::InternalError("allocate cachelib item failed");
    }
    std::memcpy(handle->getMemory(), value, size);
    _cache->insertOrReplace(handle);
    return Status::OK();
}

StatusOr<size_t> FbCacheLib::read_cache(const std::string& key, char* value, size_t off, size_t size) {
    // TODO:
    // 1. check chain item
    // 2. replace with async methods
    auto handle = _cache->find(key);
    if (!handle) {
        return Status::NotFound("not found cachelib item");
    }
    std::memcpy(value, (char*)handle->getMemory() + off, size);
    if (handle->hasChainedItem()) {
    }
    return size;
}

Status FbCacheLib::remove_cache(const std::string& key) {
    _cache->remove(key);
    return Status::OK();
}

Status FbCacheLib::destroy() {
    if (_cache) {
        _cache.reset();
    }
    return Status::OK();
}

FbCacheLib::~FbCacheLib() {
    destroy();
}

} // namespace starrocks
