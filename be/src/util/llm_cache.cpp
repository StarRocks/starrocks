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

#include "llm_cache.h"

#include "util/lru_cache.h"
#include "util/md5.h"

namespace starrocks {

static const std::string CACHE_PREFIX = "llm_cache";

std::string generate_cache_key(const std::string& prompt, const ModelConfig& config) {
    std::ostringstream config_stream;
    config_stream << config.model << ":" << config.temperature << ":" << config.max_tokens << ":" << config.top_p << ":"
                  << config.timeout_ms;

    std::string full_content = CACHE_PREFIX + "|" + config_stream.str() + "|" + prompt;

    Md5Digest digest;
    digest.update(full_content.c_str(), full_content.length());
    digest.digest();
    return digest.hex();
}

void llm_cache_value_deleter(const CacheKey& /* key */, void* value) {
    delete static_cast<LLMCacheValue*>(value);
}

void LLMCache::init(size_t capacity) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_cache) {
        delete _cache.release();
    }
    _cache.reset(new_lru_cache(capacity));

    _cache_hits = 0;
    _cache_misses = 0;
    _total_requests = 0;
}

void LLMCache::set_capacity(size_t capacity) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_cache) {
        _cache->set_capacity(capacity);
    }
}

LLMCacheValue* LLMCache::lookup(const CacheKey& key) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_cache) {
        return nullptr;
    }
    _total_requests++;

    auto* handle = _cache->lookup(key);
    if (handle) {
        LLMCacheValue* cache_value = static_cast<LLMCacheValue*>(_cache->value(handle));
        _cache_hits++;
        _cache->release(handle);
        return cache_value;
    }
    _cache_misses++;
    return nullptr;
}

void LLMCache::insert(const std::string& cache_key, std::string response) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_cache) {
        return;
    }

    auto cache_value = std::make_unique<LLMCacheValue>();
    cache_value->response = std::move(response);
    cache_value->cache_key_str = cache_key;

    size_t charge = sizeof(LLMCacheValue) + cache_value->response.size() + cache_value->cache_key_str.size();
    auto* new_handle = _cache->insert(CacheKey(cache_key), cache_value.get(), charge, &llm_cache_value_deleter);

    if (new_handle) {
        _cache->release(new_handle);
        // Transfer ownership to cache
        cache_value.release();
    }
}

void LLMCache::release_cache() {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_cache) {
        delete _cache.release();
    }
}

CacheMetrics LLMCache::get_metrics() const {
    std::lock_guard<std::mutex> lock(_mutex);
    CacheMetrics metrics;
    metrics.cache_hits = _cache_hits;
    metrics.cache_misses = _cache_misses;
    metrics.total_requests = _total_requests;
    metrics.hit_rate = _total_requests > 0 ? static_cast<double>(_cache_hits) / _total_requests : 0.0;

    if (_cache) {
        metrics.cache_size = _cache->get_memory_usage();
        metrics.cache_capacity = _cache->get_capacity();
    }

    return metrics;
}

} // namespace starrocks
