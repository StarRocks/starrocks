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
#include <mutex>

#include "exprs/ai_functions.h"
#include "util/lru_cache.h"

namespace starrocks {

struct LLMCacheValue {
    std::string response;
    std::string cache_key_str;
};

struct CacheMetrics {
    size_t cache_hits = 0;
    size_t cache_misses = 0;
    size_t total_requests = 0;
    double hit_rate = 0.0;
    size_t cache_size = 0;
    size_t cache_capacity = 0;
};

std::string generate_cache_key(const std::string& prompt, const ModelConfig& config);

void llm_cache_value_deleter(const CacheKey& key, void* value);

class LLMCache {
public:
    LLMCache() = default;

    Cache* get_cache() const { return _cache.get(); }
    void init(size_t capacity);
    void set_capacity(size_t capacity);
    std::optional<std::string> lookup(const CacheKey& key);
    void insert(const std::string& key, std::string response);
    void release_cache();
    CacheMetrics get_metrics() const;

private:
    std::unique_ptr<Cache> _cache;
    mutable std::mutex _mutex;

    std::atomic<size_t> _cache_hits{0};
    std::atomic<size_t> _cache_misses{0};
    std::atomic<size_t> _total_requests{0};
};

} // namespace starrocks
