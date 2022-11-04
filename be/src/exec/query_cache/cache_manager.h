// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once
#include <memory>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
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

    CacheValue(int64_t populate_time, int64_t cache_version, CacheResult&& cache_result)
            : latest_hit_time(0),
              hit_count(0),
              populate_time(populate_time),
              version(cache_version),
              result(cache_result) {}

    CacheValue(const CacheValue& that)
            : latest_hit_time(that.latest_hit_time),
              hit_count(that.hit_count),
              populate_time(that.populate_time),
              version(that.version),
              result(that.result) {}

    CacheValue& operator=(const CacheValue& that) {
        this->latest_hit_time = that.latest_hit_time;
        this->hit_count = that.hit_count;
        this->populate_time = that.populate_time;
        this->version = that.version;
        this->result = that.result;
        return *this;
    }

    ~CacheValue() { result.clear(); }

    size_t size() {
        size_t value_size = 0;
        for (auto& chk : result) {
            value_size += chk->memory_usage();
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
    // vacuum cache by invalidate all cache entries
    void invalidate_all();

private:
    ShardedLRUCache _cache;
};
} // namespace query_cache
} // namespace starrocks
