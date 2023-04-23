// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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

class FbCacheLib : public KvCache {
public:
    using Cache = facebook::cachelib::LruAllocator;
    using PoolId = facebook::cachelib::PoolId;
    using ReadHandle = facebook::cachelib::LruAllocator::ReadHandle;

    FbCacheLib() = default;
    ~FbCacheLib() override = default;

    Status init(const CacheOptions& options) override;

    Status write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) override;

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
