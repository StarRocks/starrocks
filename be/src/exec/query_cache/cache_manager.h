// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once
#include <memory>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "util/lru_cache.h"
#include "util/slice.h"

namespace starrocks {
namespace query_cache {
class CacheManager;
using CacheManagerRawPtr = CacheManager*;
using CacheManagerPtr = std::shared_ptr<CacheManager>;

using CacheResult = std::vector<vectorized::ChunkPtr>;

struct CacheValue {
    int64_t latest_hit_time;
    int64_t hit_count;
    int64_t populate_time;
    int64_t version;
    CacheResult result;
    size_t size() {
        size_t value_size = 0;
        for (auto& chk : result) {
            value_size += chk->bytes_usage();
        }
        return value_size;
    }
};

class CacheManager {
public:
    explicit CacheManager(size_t capacity);
    ~CacheManager() = default;
    Status populate(const std::string& key, const CacheValue& value);
    StatusOr<CacheValue> probe(const std::string& key);
    size_t memory_usage();
    size_t capacity();
    size_t lookup_count();
    size_t hit_count();

private:
    ShardedLRUCache _cache;
};
} // namespace query_cache
} // namespace starrocks
