// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/status.h"
#include "block_cache/kv_cache.h"
#include "star_cache/star_cache.h"

namespace starrocks {

class StarCacheLib : public KvCache {
public:
    StarCacheLib() = default;
    ~StarCacheLib() override = default;

    Status init(const CacheOptions& options) override;

    Status write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) override;

    StatusOr<size_t> read_cache(const std::string& key, char* value, size_t off, size_t size) override;

    Status remove_cache(const std::string& key) override;

    Status shutdown() override;

private:
    std::unique_ptr<starcache::StarCache> _cache = nullptr;
};

} // namespace starrocks
