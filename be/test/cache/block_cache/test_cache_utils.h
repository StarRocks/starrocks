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

#include <fmt/format.h>

#include "cache/block_cache/block_cache.h"
#include "common/logging.h"

namespace starrocks {

constexpr size_t KB = 1024;
constexpr size_t MB = KB * 1024;
constexpr size_t GB = MB * 1024;

CacheOptions create_simple_options(size_t block_size, size_t mem_quota, ssize_t disk_quota = -1,
                                   const std::string& engine = "starcache") {
    CacheOptions options;
    options.mem_space_size = mem_quota;
    if (disk_quota > 0) {
        options.disk_spaces.push_back({.path = "./block_disk_cache", .size = (size_t)disk_quota});
    }
    options.engine = engine;
    options.enable_checksum = false;
    options.max_concurrent_inserts = 1500000;
    options.max_flying_memory_mb = 100;
    options.enable_tiered_cache = true;
    options.block_size = block_size;
    options.skip_read_factor = 1.0;
    return options;
}

std::shared_ptr<BlockCache> create_cache(const CacheOptions& options) {
    std::shared_ptr<BlockCache> cache(new BlockCache);
    Status status = cache->init(options);
    EXPECT_TRUE(status.ok());
    return cache;
}

} // namespace starrocks
