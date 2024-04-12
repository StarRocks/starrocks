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
    std::string engine;
    size_t max_concurrent_inserts = 0;
    size_t max_flying_memory_mb = 0;
    double scheduler_threads_per_cpu = 0;
    double skip_read_factor = 0;
};

struct WriteCacheOptions {
    // If ttl_seconds=0 (default), no ttl restriction will be set. If an old one exists, remove it.
    uint64_t ttl_seconds = 0;
    // If overwrite=true, the cache value will be replaced if it already exists.
    bool overwrite = false;
    bool async = false;
    // When allow_zero_copy=true, it means the caller can ensure the target buffer not be released before
    // the write finish. So the cache library can use the buffer directly without copying it to another buffer.
    bool allow_zero_copy = false;
    std::function<void(int, const std::string&)> callback = nullptr;

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

int64_t parse_conf_datacache_mem_size(const std::string& conf_mem_size_str, int64_t mem_limit);

int64_t parse_conf_datacache_disk_size(const std::string& disk_path, const std::string& disk_size_str,
                                       int64_t disk_limit);

Status parse_conf_datacache_disk_paths(const std::string& config_path, std::vector<std::string>* paths,
                                       bool ignore_broken_disk);

Status parse_conf_datacache_disk_spaces(const std::string& config_disk_path, const std::string& config_disk_size,
                                        bool ignore_broken_disk, std::vector<DirSpace>* disk_spaces);

} // namespace starrocks
