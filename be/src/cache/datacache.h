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

#pragma once

#include "cache/block_cache/block_cache.h"
#include "cache/local_cache_engine.h"
#include "common/status.h"

namespace starrocks {

class Status;
class StorePath;
class RemoteCacheEngine;
class CacheOptions;
class GlobalEnv;
class DiskSpaceMonitor;
class MemSpaceMonitor;
class StoragePageCache;
class Cache;

class DataCache {
public:
    static DataCache* GetInstance();

    Status init(const std::vector<StorePath>& store_paths);
    void destroy();

    void try_release_resource_before_core_dump();

    void set_local_cache(std::shared_ptr<LocalCacheEngine> local_cache) { _local_cache = std::move(local_cache); }
    void set_page_cache(std::shared_ptr<StoragePageCache> page_cache) { _page_cache = std::move(page_cache); }

    LocalCacheEngine* local_cache() { return _local_cache.get(); }
    BlockCache* block_cache() const { return _block_cache.get(); }
    void set_block_cache(std::shared_ptr<BlockCache> block_cache) { _block_cache = std::move(block_cache); }
    StoragePageCache* page_cache() const { return _page_cache.get(); }
    std::shared_ptr<StoragePageCache> page_cache_ptr() const { return _page_cache; }
    bool page_cache_available() const;

    StatusOr<int64_t> get_storage_page_cache_limit();
    int64_t check_storage_page_cache_limit(int64_t storage_cache_limit);

    bool adjust_mem_capacity(int64_t delta, size_t min_capacity);
    size_t get_mem_capacity() const;

private:
    StatusOr<CacheOptions> _init_cache_options();
#if defined(WITH_STARCACHE)
    Status _init_starcache(CacheOptions* cache_options);
    Status _init_peer_cache(const CacheOptions& cache_options);
#endif
    Status _init_lrucache_engine(const CacheOptions& cache_options);
    Status _init_page_cache();

    GlobalEnv* _global_env;
    std::vector<StorePath> _store_paths;

    // cache engine
    std::shared_ptr<LocalCacheEngine> _local_cache;
    std::shared_ptr<RemoteCacheEngine> _remote_cache;

    std::shared_ptr<BlockCache> _block_cache;
    std::shared_ptr<StoragePageCache> _page_cache;

    std::shared_ptr<DiskSpaceMonitor> _disk_space_monitor;
    std::shared_ptr<MemSpaceMonitor> _mem_space_monitor;
};

} // namespace starrocks