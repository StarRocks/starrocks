// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/lru_eviction_policy.h"

namespace starrocks {

template <typename T>
LruEvictionPolicy<T>::~LruEvictionPolicy() {
    // We do not plan to support the persistence of the eviction policy in this period,
    // so clear the elements directly.
    _lru_cache->prune();
}

template <typename T>
char* makeup_key(const T& id) {
    char* key = new char[sizeof(T)];
    memcpy(key, &id, sizeof(T));
    return key;
}

inline void free_key(char* key) {
    delete[] key;
}

template <typename T>
bool LruEvictionPolicy<T>::add(const T& id) {
    auto deleter = [](const CacheKey& key, void* value) {};
    char* key = makeup_key(id);
    Cache::Handle* h = _lru_cache->insert(CacheKey(key, sizeof(T)), nullptr, 1, deleter);
    free_key(key);
    if (h) {
        _lru_cache->release(h);
        return true;
    }
    return false;
}

template <typename T>
bool LruEvictionPolicy<T>::touch(const T& id) {
    char* key = makeup_key(id);
    Cache::Handle* h = _lru_cache->lookup(CacheKey(key, sizeof(T)));
    free_key(key);
    if (h) {
        _lru_cache->release(h);
        return true;
    }
    return false;
}

template <typename T>
void LruEvictionPolicy<T>::evict(size_t count, std::vector<T>* evicted) {
    std::vector<Cache::Handle*> handles;
    _lru_cache->evict(count, &handles);
    for (auto h : handles) {
        auto lru_h = reinterpret_cast<LRUHandle*>(h);
        T* eid = reinterpret_cast<T*>(lru_h->key_data);
        evicted->push_back(*eid);
    }
}

template <typename T>
void LruEvictionPolicy<T>::evict_for(const T& id, size_t count, std::vector<T>* evicted) {
    char* key = makeup_key(id);
    std::vector<Cache::Handle*> handles;
    _lru_cache->evict_for(CacheKey(key, sizeof(T)), count, &handles);
    for (auto h : handles) {
        auto lru_h = reinterpret_cast<LRUHandle*>(h);
        T* eid = reinterpret_cast<T*>(lru_h->key_data);
        evicted->push_back(*eid);
        lru_h->free();
    }
    free_key(key);
}

template <typename T>
void LruEvictionPolicy<T>::remove(const T& id) {
    char* key = makeup_key(id);
    _lru_cache->erase(CacheKey(key, sizeof(T)));
    free_key(key);
}

template <typename T>
void LruEvictionPolicy<T>::clear() {
    _lru_cache->prune();
}

} // namespace starrocks
