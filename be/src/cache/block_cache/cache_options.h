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

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "common/status.h"

namespace starrocks {

// Options to control how to create DataCache instance
struct DataCacheOptions {
    bool enable_datacache = false;
    bool enable_cache_select = false;
    bool enable_populate_datacache = false;
    bool enable_datacache_async_populate_mode = false;
    bool enable_datacache_io_adaptor = false;
    int64_t modification_time = 0;
    int32_t datacache_evict_probability = 100;
    int8_t datacache_priority = 0;
    int64_t datacache_ttl_seconds = 0;
};

struct DirSpace {
    std::string path;
    size_t size;
};

struct CacheOptions {
    // basic
    size_t mem_space_size = 0;
    std::vector<DirSpace> disk_spaces;
    std::string meta_path;

    // advanced
    size_t block_size = 0;
    bool enable_checksum = false;
    bool enable_direct_io = false;
    bool enable_tiered_cache = true;
    bool enable_datacache_persistence = false;
    std::string engine;
    size_t max_concurrent_inserts = 0;
    size_t max_flying_memory_mb = 0;
    double scheduler_threads_per_cpu = 0;
    double skip_read_factor = 0;
    uint32_t inline_item_count_limit = 0;
    std::string eviction_policy;
};

struct WriteCacheOptions {
    int8_t priority = 0;
    // If ttl_seconds=0 (default), no ttl restriction will be set. If an old one exists, remove it.
    uint64_t ttl_seconds = 0;
    // If overwrite=true, the cache value will be replaced if it already exists.
    bool overwrite = false;
    bool async = false;
    // When allow_zero_copy=true, it means the caller can ensure the target buffer not be released before
    // the write finish. So the cache library can use the buffer directly without copying it to another buffer.
    bool allow_zero_copy = false;
    std::function<void(int, const std::string&)> callback = nullptr;

    // The probability to evict other items if the cache space is full, which can help avoid frequent cache replacement
    // and improve cache hit rate sometimes.
    // It is expressed as a percentage. If evict_probability is 10, it means the probability to evict other data is 10%.
    int32_t evict_probability = 100;

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

struct ReadCacheOptions {
    bool use_adaptor = false;

    struct Stats {
        int64_t read_mem_bytes = 0;
        int64_t read_disk_bytes = 0;
    } stats;
};
} // namespace starrocks
