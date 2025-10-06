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

#include "cache/cache_metrics.h"
#include "cache/disk_cache/io_buffer.h"
#include "common/status.h"

namespace starrocks {

struct DirSpace {
    std::string path;
    size_t size;
};

struct DiskCacheOptions {
    // basic
    size_t mem_space_size = 0;
    std::vector<DirSpace> dir_spaces;
    std::string meta_path;

    // advanced
    size_t block_size = 0;
    bool enable_checksum = false;
    bool enable_direct_io = false;
    bool enable_datacache_persistence = false;
    size_t max_concurrent_inserts = 0;
    size_t max_flying_memory_mb = 0;
    double scheduler_threads_per_cpu = 0;
    double skip_read_factor = 0;
    uint32_t inline_item_count_limit = 0;
    std::string eviction_policy;
};

struct DiskCacheWriteOptions {
    int8_t priority = 0;
    // If ttl_seconds=0 (default), no ttl restriction will be set. If an old one exists, remove it.
    uint64_t ttl_seconds = 0;
    // If overwrite=true, the cache value will be replaced if it already exists.
    bool overwrite = false;
    bool async = false;
    // When allow_zero_copy=true, it means the caller can ensure the target buffer not be released before
    // to write finish. So the cache library can use the buffer directly without copying it to another buffer.
    bool allow_zero_copy = false;
    std::function<void(int, const std::string&)> callback = nullptr;

    // The base frequency for target cache.
    // When using multiple segment lru, a higher frequency may cause the cache is written to warm segment directly.
    // For the default cache options, that `lru_segment_freq_bits` is 0:
    // * The default `frequency=0` indicates the cache will be written to cold segment.
    // * A frequency value greater than 0 indicates writing this cache directly to the warm segment.
    int8_t frequency = 0;

    struct Stats {
        int64_t write_mem_bytes = 0;
        int64_t write_disk_bytes = 0;
    } stats;
};

struct DiskCacheReadOptions {
    bool use_adaptor = false;
    std::string remote_host;
    int32_t remote_port;

    struct Stats {
        int64_t read_mem_bytes = 0;
        int64_t read_disk_bytes = 0;
    } stats;
};

struct DataCacheDiskMetrics {
    DataCacheStatus status;

    size_t disk_quota_bytes;
    size_t disk_used_bytes;
};

class LocalDiskCacheEngine {
public:
    virtual ~LocalDiskCacheEngine() = default;

    virtual bool is_initialized() const = 0;

    // Write data to cache
    virtual Status write(const std::string& key, const IOBuffer& buffer, DiskCacheWriteOptions* options) = 0;

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned.
    virtual Status read(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                        DiskCacheReadOptions* options) = 0;

    virtual bool exist(const std::string& key) const = 0;

    // Remove data from cache.
    virtual Status remove(const std::string& key) = 0;

    // Update the datacache disk space information, such as disk quota or disk path.
    virtual Status update_disk_spaces(const std::vector<DirSpace>& spaces) = 0;

    // Update the datacache inline cache count limit
    virtual Status update_inline_cache_count_limit(int32_t limit) = 0;

    virtual const DataCacheDiskMetrics cache_metrics() const = 0;

    virtual void record_read_remote(size_t size, int64_t latency_us) = 0;

    virtual void record_read_cache(size_t size, int64_t latency_us) = 0;

    virtual Status shutdown() = 0;

    virtual bool has_disk_cache() const = 0;
    virtual bool available() const = 0;
    virtual void disk_spaces(std::vector<DirSpace>* spaces) const = 0;

    // Get the lookup count, including cache hit count and cache miss count.
    virtual size_t lookup_count() const = 0;

    // Get the cache hit count.
    virtual size_t hit_count() const = 0;

    // Remove all cache entries that are not actively in use.
    virtual Status prune() = 0;
};

} // namespace starrocks
