// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "block_cache/cache_options.h"
#include "common/status.h"

namespace starrocks {

class KvCache {
public:
    virtual ~KvCache() = default;

    // Init KV cache
    virtual Status init(const CacheOptions& options) = 0;

    // Write data to cache
    virtual Status write_cache(const std::string& key, const char* value, size_t size, size_t ttl_seconds) = 0;

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned.
    virtual StatusOr<size_t> read_cache(const std::string& key, char* value, size_t off, size_t size) = 0;

    // Remove data from cache. The offset must be aligned by block size
    virtual Status remove_cache(const std::string& key) = 0;

    virtual Status destroy() = 0;
};

} // namespace starrocks
