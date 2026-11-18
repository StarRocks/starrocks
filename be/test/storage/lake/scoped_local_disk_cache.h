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

#ifdef WITH_STARCACHE
#include <memory>
#include <string>

#include "cache/datacache.h"
#include "cache/disk_cache/starcache_engine.h"
#include "common/logging.h"
#include "fs/fs_util.h"

namespace starrocks::lake {

// Gives DataCache a local disk cache in |dir| while it lives. Unit tests run with the datacache off.
class ScopedLocalDiskCache {
public:
    explicit ScopedLocalDiskCache(const std::string& dir) : _dir(dir) {
        CHECK_OK(fs::create_directories(_dir));
        DiskCacheOptions options;
        options.dir_spaces.push_back({.path = _dir, .size = 64 * 1024 * 1024});
        options.block_size = 256 * 1024;
        options.max_concurrent_inserts = 1500000;
        options.max_flying_memory_mb = 100;
        options.skip_read_factor = 1.0;
        options.inline_item_count_limit = 1000;
        auto cache = std::make_shared<StarCacheEngine>();
        CHECK_OK(cache->init(options));
        DataCache::GetInstance()->set_local_disk_cache(std::move(cache));
    }
    ~ScopedLocalDiskCache() {
        DataCache::GetInstance()->set_local_disk_cache(nullptr);
        (void)fs::remove_all(_dir);
    }

private:
    std::string _dir;
};

} // namespace starrocks::lake
#endif // WITH_STARCACHE
