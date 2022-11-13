// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/hashtable_access_index.h"

#include "common/logging.h"
#include "common/statusor.h"

namespace starrocks {

bool HashTableAccessIndex::insert(const CacheId& id, const CacheItemPtr& item) {
    _cache_items.update(id, item);
    return true;
}

CacheItemPtr HashTableAccessIndex::find(const CacheId& id) {
    CacheItemPtr cache_item(nullptr);
    _cache_items.find(id, &cache_item);
    return cache_item;
}

bool HashTableAccessIndex::remove(const CacheId& id) {
    return _cache_items.remove(id);
}

} // namespace starrocks
