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

#include <memory>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "util/lru_cache.h"
#include "util/slice.h"

namespace starrocks::query_cache {
class CacheManager;
using CacheManagerRawPtr = CacheManager*;
using CacheManagerPtr = std::shared_ptr<CacheManager>;

using CacheResult = std::vector<ChunkPtr>;

struct CacheValue {
    int64_t latest_hit_time{0};
    int64_t hit_count{0};
    int64_t populate_time;
    int64_t version;
    CacheResult result;

    CacheValue(int64_t populate_time, int64_t cache_version, CacheResult&& cache_result)
            : populate_time(populate_time), version(cache_version), result(cache_result) {}

    CacheValue(const CacheValue& that)

            = default;

    CacheValue& operator=(const CacheValue& that) = default;

    ~CacheValue() { result.clear(); }

    size_t size() {
        // zero-charge cache entry can not be purged in LRU cache, so size of CacheValue must be at least
        // greater than zero, so add sizeof(CacheValue) to size.
        size_t value_size = sizeof(CacheValue);
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
    void populate(const std::string& key, const CacheValue& value);
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
} // namespace starrocks::query_cache
