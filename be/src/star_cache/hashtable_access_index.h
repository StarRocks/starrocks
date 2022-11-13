// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <shared_mutex>
#include <atomic>
#include "common/status.h"
#include "star_cache/thread_safe_hash_map.h"
#include "star_cache/access_index.h"

namespace starrocks {

class HashTableAccessIndex : public AccessIndex {
public:
    bool insert(const CacheId& id, const CacheItemPtr& item) override;
    CacheItemPtr find(const CacheId& id) override;
    bool remove(const CacheId& id) override;

private:
    ThreadSafeHashMap<CacheId, CacheItemPtr> _cache_items;
};

} // namespace starrocks
