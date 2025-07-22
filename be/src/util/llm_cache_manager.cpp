#include "llm_cache_manager.h"
#include "util/lru_cache.h"

namespace starrocks {

constexpr const char* CACHE_PREFIX = "LLM_RESP";

std::string generate_cache_key(const std::string& prompt, const ModelConfig& config) {
    std::string key_str = strings::Substitute("$0:$1:$2:$3:$4",
        CACHE_PREFIX,
        config.model,
        config.temperature,
        config.max_tokens,
        config.top_p);
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
    if (!_cache) {
        _cache.reset(new_lru_cache(capacity));
    }
}

void LLMCacheManager::set_capacity(size_t capacity) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_cache) {
        _cache->set_capacity(capacity);
    }
}

} // namespace starrocks