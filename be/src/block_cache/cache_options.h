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

#include <string>
#include <vector>

namespace starrocks {

struct DirSpace {
    std::string path;
    size_t size;
};

struct CacheOptions {
    // basic
    size_t mem_space_size;
    std::vector<DirSpace> disk_spaces;
    std::string meta_path;

    // advanced
    size_t block_size;
    bool checksum;
    std::string engine;
    size_t max_concurrent_inserts;
    // The following options are only valid for cachelib engine currently
    size_t max_parcel_memory_mb;
    uint8_t lru_insertion_point;

    bool enable_page_cache;
    bool enable_cache_adaptor;
    size_t skip_read_factor;
};

struct WriteCacheOptions {
    // If ttl_seconds=0 (default), no ttl restriction will be set. If an old one exists, remove it.
    uint64_t ttl_seconds = 0;
    // If overwrite=true, the cache value will be replaced if it already exists.
    bool overwrite = true;

    struct Stats {
        int64_t write_mem_bytes = 0;
        int64_t write_disk_bytes = 0;
    } stats;
};

struct ReadCacheOptions {
    struct Stats {
        int64_t read_mem_bytes = 0;
        int64_t read_disk_bytes = 0;
    } stats;
};

} // namespace starrocks
