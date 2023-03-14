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

#include "block_cache/fb_cachelib.h"

#include "common/logging.h"
#include "common/statusor.h"
#include "gutil/strings/fastmem.h"
#include "util/filesystem_util.h"

namespace starrocks {

Status FbCacheLib::init(const CacheOptions& options) {
    Cache::Config config;
    config.setCacheSize(options.mem_space_size).setCacheName("default cache").setAccessConfig({25, 10}).validate();

    std::vector<std::string> nvm_files;
    if (!options.disk_spaces.empty()) {
        Cache::NvmCacheConfig nvmConfig;
        nvmConfig.navyConfig.setBlockSize(4096);

        for (auto& dir : options.disk_spaces) {
            nvm_files.emplace_back(dir.path + "/cachelib_data");
            // If exist, truncate it to empty file
            FileSystemUtil::resize_file(nvm_files.back(), 0);
        }
        if (nvm_files.size() == 1) {
            nvmConfig.navyConfig.setSimpleFile(nvm_files[0], options.disk_spaces[0].size, false);
        } else {
            nvmConfig.navyConfig.setRaidFiles(nvm_files, options.disk_spaces[0].size, false);
        }
        nvmConfig.navyConfig.blockCache().setRegionSize(16 * 1024 * 1024);
        nvmConfig.navyConfig.blockCache().setDataChecksum(options.checksum);
        nvmConfig.navyConfig.setMaxParcelMemoryMB(options.max_parcel_memory_mb);
        nvmConfig.navyConfig.setMaxConcurrentInserts(options.max_concurrent_inserts);
        config.enableNvmCache(nvmConfig);
    }

    _cache = std::make_unique<Cache>(config);
    _default_pool = _cache->addPool("default pool", _cache->getCacheMemoryStats().cacheSize);
    _meta_path = options.meta_path;
    return Status::OK();
}

Status FbCacheLib::write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) {
    // TODO: check size for chain item
    auto handle = _cache->allocate(_default_pool, key, size);
    if (!handle) {
        return Status::InternalError("allocate cachelib item failed");
    }
    // std::memcpy(handle->getMemory(), value, size);
    strings::memcpy_inlined(handle->getMemory(), value, size);
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
    DCHECK((off + size) <= handle->getSize());
    // std::memcpy(value, (char*)handle->getMemory() + off, size);
    strings::memcpy_inlined(value, (char*)handle->getMemory() + off, size);

    if (handle->hasChainedItem()) {
    }
    return size;
}

Status FbCacheLib::remove_cache(const std::string& key) {
    _cache->remove(key);
    return Status::OK();
}

std::unordered_map<std::string, double> FbCacheLib::cache_stats() {
    const auto navy_stats = _cache->getNvmCacheStatsMap().toMap();
    return navy_stats;
}

Status FbCacheLib::shutdown() {
    if (_cache) {
        _dump_cache_stats();
    }
    return Status::OK();
}

void FbCacheLib::_dump_cache_stats() {
    std::ofstream of;
    of.open(_meta_path + "/cachelib_stat", std::ios::out | std::ios::trunc);
    const auto stats = cache_stats();
    for (auto& stat : stats) {
        of << stat.first << " : " << stat.second << "\n";
    }
    of.close();
}

} // namespace starrocks
