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

#include <memory>
#include <string>
#include <vector>

#include "cache/disk_cache/local_disk_cache_engine.h"
#include "cache/mem_cache/local_mem_cache_engine.h"
#include "common/status.h"

namespace starrocks {

class BlockCache;
struct BlockCacheOptions;
class Status;
class RemoteCacheEngine;
struct RemoteCacheOptions;
struct DiskCacheOptions;
struct MemCacheOptions;
class DiskSpaceMonitor;
class MetricRegistry;
class MemTracker;
class MemSpaceMonitor;
class StoragePageCache;
class Cache;
class BrpcStubCache;

struct DataCacheInitOptions {
    std::vector<std::string> storage_root_paths;
    MetricRegistry* metrics = nullptr;
    int64_t process_mem_limit = -1;
    MemTracker* process_mem_tracker = nullptr;
};

class DataCache {
public:
    static DataCache* GetInstance();

    Status init(const DataCacheInitOptions& options);
    void attach_peer_cache_stub_cache(BrpcStubCache* brpc_stub_cache);
    void destroy();

    void try_release_resource_before_core_dump();

    void set_local_disk_cache(std::shared_ptr<LocalDiskCacheEngine> local_disk_cache) {
        _local_disk_cache = std::move(local_disk_cache);
    }
    void set_page_cache(std::shared_ptr<StoragePageCache> page_cache) { _page_cache = std::move(page_cache); }

    LocalMemCacheEngine* local_mem_cache() { return _local_mem_cache.get(); }
    LocalDiskCacheEngine* local_disk_cache() { return _local_disk_cache.get(); }
    BlockCache* block_cache() const { return _block_cache.get(); }
    void set_block_cache(std::shared_ptr<BlockCache> block_cache) { _block_cache = std::move(block_cache); }
    StoragePageCache* page_cache() const { return _page_cache.get(); }
    std::shared_ptr<StoragePageCache> page_cache_ptr() const { return _page_cache; }
    bool page_cache_available() const;

    StatusOr<int64_t> get_datacache_limit();
    int64_t check_datacache_limit(int64_t datacache_limit);

    bool adjust_mem_capacity(int64_t delta, size_t min_capacity);
    size_t get_mem_capacity() const;

private:
    StatusOr<MemCacheOptions> _init_mem_cache_options();
    RemoteCacheOptions _init_remote_cache_options();
    BlockCacheOptions _init_block_cache_options();

#if defined(WITH_STARCACHE)
    StatusOr<DiskCacheOptions> _init_disk_cache_options();
    Status _init_starcache_engine(DiskCacheOptions* cache_options);
    Status _init_peer_cache(const RemoteCacheOptions& cache_options);
#endif
    Status _init_lrucache_engine(const MemCacheOptions& cache_options);
    Status _init_page_cache(MetricRegistry* metrics);

    std::vector<std::string> _storage_root_paths;
    int64_t _process_mem_limit = -1;
    MemTracker* _process_mem_tracker = nullptr;

    // cache engine
    std::shared_ptr<LocalMemCacheEngine> _local_mem_cache;
    std::shared_ptr<LocalDiskCacheEngine> _local_disk_cache;
    std::shared_ptr<RemoteCacheEngine> _remote_cache;

    std::shared_ptr<BlockCache> _block_cache;
    std::shared_ptr<StoragePageCache> _page_cache;

    std::shared_ptr<DiskSpaceMonitor> _disk_space_monitor;
    std::shared_ptr<MemSpaceMonitor> _mem_space_monitor;
};

} // namespace starrocks
