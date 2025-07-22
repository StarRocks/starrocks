#pragma once

#include <mutex>
#include <memory>
#include "util/lru_cache.h"
#include "exprs/ai_functions.h"

namespace starrocks {

struct LLMCacheValue {
    std::string response;
    std::string cache_key_str;
};

std::string generate_cache_key(const std::string& prompt, const ModelConfig& config);

void llm_cache_value_deleter(const CacheKey& key, void* value);

class LLMCacheManager {
public:
    static LLMCacheManager* instance();

    Cache* get_cache() const { return _cache.get(); }
    void init(size_t capacity);
    void set_capacity(size_t capacity);

private:
    LLMCacheManager() = default;
    std::unique_ptr<Cache> _cache;
    mutable std::mutex _mutex;
};

} // namespace starrocks
