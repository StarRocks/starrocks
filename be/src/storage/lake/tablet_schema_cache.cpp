// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "tablet_schema_cache.h"

namespace starrocks::lake {

static void tablet_schema_deleter(const CacheKey& key, void* value) {
    TabletSchemaPtr* ptr = static_cast<TabletSchemaPtr*>(value);
    delete ptr;
}

bool TabletSchemaCache::fill_schema_cache(int64_t key, const TabletSchemaPtr& ptr, int mem_size) {
    auto value_ptr = static_cast<void*>(new std::shared_ptr<const TabletSchema>(ptr));
    Cache::Handle* handle = _schema_cache->insert(CacheKey(reinterpret_cast<const char*>(&key), sizeof(key)), value_ptr,
                                                  mem_size, tablet_schema_deleter);
    bool res = true;
    if (handle == nullptr) {
        res = false;
    } else {
        _schema_cache->release(handle);
    }
    return res;
}

TabletSchemaPtr TabletSchemaCache::lookup_schema_cache(int64_t key) {
    auto handle = _schema_cache->lookup(CacheKey(reinterpret_cast<const char*>(&key), sizeof(key)));
    if (handle == nullptr) {
        return nullptr;
    }
    auto ptr = *(static_cast<TabletSchemaPtr*>(_schema_cache->value(handle)));
    _schema_cache->release(handle);
    return ptr;
}

void TabletSchemaCache::erase_schema_cache(int64_t key) {
    _schema_cache->erase(CacheKey(reinterpret_cast<const char*>(&key), sizeof(key)));
}

void TabletSchemaCache::prune_schema_cache() {
    _schema_cache->prune();
}

} // namespace starrocks::lake
