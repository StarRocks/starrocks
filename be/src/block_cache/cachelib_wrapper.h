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

// The following macros only used to avoid some variable and function conflicts
// caused by cachelib and related dependencies, and these macros will be removed
// once the cachelib be deprecated.

#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 0
#endif

#ifndef JEMALLOC_NO_RENAME
#define JEMALLOC_NO_RENAME 1
#endif

#include <cachelib/allocator/CacheAllocator.h>

#include "block_cache/kv_cache.h"
#include "common/status.h"

namespace starrocks {

class CacheLibWrapper : public KvCache {
public:
    using Cache = facebook::cachelib::LruAllocator;
    using PoolId = facebook::cachelib::PoolId;
    using ReadHandle = facebook::cachelib::LruAllocator::ReadHandle;

    CacheLibWrapper() = default;
    ~CacheLibWrapper() override = default;

    Status init(const CacheOptions& options) override;

    Status write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds,
                       bool overwrite) override;

    StatusOr<size_t> read_cache(const std::string& key, char* value, size_t off, size_t size) override;

    Status remove_cache(const std::string& key) override;

    std::unordered_map<std::string, double> cache_stats() override;

    Status shutdown() override;

private:
    void _dump_cache_stats();

    std::unique_ptr<Cache> _cache = nullptr;
    PoolId _default_pool;
    std::string _meta_path;
};

} // namespace starrocks
