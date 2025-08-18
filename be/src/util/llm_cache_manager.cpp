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

#include "llm_cache_manager.h"

#include "util/lru_cache.h"

namespace starrocks {

constexpr const char* CACHE_PREFIX = "LLM_RESP";

std::string generate_cache_key(const std::string& prompt, const ModelConfig& config) {
    std::string key_str = strings::Substitute("$0:$1:$2:$3:$4", CACHE_PREFIX, config.model, config.temperature,
                                              config.max_tokens, config.top_p);
    key_str.append("|");
    key_str.append(prompt);
    return key_str;
}

void llm_cache_value_deleter(const CacheKey& /* key */, void* value) {
    delete static_cast<LLMCacheValue*>(value);
}

LLMCacheManager* LLMCacheManager::instance() {
    static LLMCacheManager inst;
    return &inst;
}

void LLMCacheManager::init(size_t capacity) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_cache) {
        delete _cache.release();
    }
    _cache.reset(new_lru_cache(capacity));
}

void LLMCacheManager::set_capacity(size_t capacity) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_cache) {
        _cache->set_capacity(capacity);
    }
}

LLMCacheValue* LLMCacheManager::lookup(const CacheKey& key) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_cache) {
        return nullptr;
    }

    auto* handle = _cache->lookup(key);
    if (handle) {
        LLMCacheValue* cache_value = static_cast<LLMCacheValue*>(_cache->value(handle));
        _cache->release(handle);
        return cache_value;
    }
    return nullptr;
}

void LLMCacheManager::insert(const std::string& cache_key, std::string response) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_cache) {
        return;
    }

    auto cache_value = std::make_unique<LLMCacheValue>();
    cache_value->response = response;
    cache_value->cache_key_str = cache_key;

    size_t charge = sizeof(LLMCacheValue) + cache_value->response.size() + cache_value->cache_key_str.size();
    auto* new_handle = _cache->insert(CacheKey(cache_key), cache_value.get(), charge, &llm_cache_value_deleter);

    if (new_handle) {
        _cache->release(new_handle);
        // Transfer ownership to cache
        cache_value.release();
    }
}

void LLMCacheManager::release_cache() {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_cache) {
        delete _cache.release();
    }
}

} // namespace starrocks
