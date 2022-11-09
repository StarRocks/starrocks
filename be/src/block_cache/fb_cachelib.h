// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

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
    ~FbCacheLib();

    Status init(const CacheOptions& options) override;

    Status write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) override;

    StatusOr<size_t> read_cache(const std::string& key, char* value, size_t off, size_t size) override;

    Status remove_cache(const std::string& key) override;

    Status destroy() override;

private:
    std::unique_ptr<Cache> _cache = nullptr;
    PoolId _default_pool;
};

} // namespace starrocks
