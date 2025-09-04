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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "cache/block_cache/block_cache.h"
#include "cache/peer_cache_engine.h"
#include "cache/starcache_engine.h"
#include "common/logging.h"
#include "testutil/assert.h"

namespace starrocks {

constexpr size_t KB = 1024;
constexpr size_t MB = KB * 1024;
constexpr size_t GB = MB * 1024;

class TestCacheUtils {
public:
    static DiskCacheOptions create_simple_options(size_t block_size, ssize_t disk_quota = -1) {
        DiskCacheOptions options;
        if (disk_quota > 0) {
            options.dir_spaces.push_back({.path = "./block_disk_cache", .size = (size_t)disk_quota});
        }
        options.enable_checksum = false;
        options.max_concurrent_inserts = 1500000;
        options.max_flying_memory_mb = 100;
        options.enable_tiered_cache = true;
        options.block_size = block_size;
        options.skip_read_factor = 1.0;
        options.inline_item_count_limit = 1000;
        return options;
    }

    static std::shared_ptr<BlockCache> create_cache(const DiskCacheOptions& options) {
        BlockCacheOptions block_cache_options;
        block_cache_options.block_size = options.block_size;
        RemoteCacheOptions remote_cache_options;
        remote_cache_options.skip_read_factor = options.skip_read_factor;
        auto local_cache = std::make_shared<StarCacheEngine>();
        auto remote_cache = std::make_shared<PeerCacheEngine>();
        auto block_cache = std::make_shared<BlockCache>();
        EXPECT_OK(local_cache->init(options));
        EXPECT_OK(remote_cache->init(remote_cache_options));
        EXPECT_OK(block_cache->init(block_cache_options, local_cache, remote_cache));
        return block_cache;
    }
};

} // namespace starrocks
