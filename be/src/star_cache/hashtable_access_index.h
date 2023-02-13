// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <shared_mutex>
#include <atomic>
#include "common/status.h"
#include "star_cache/common/parallel_hash_map.h"
#include "star_cache/access_index.h"

namespace starrocks::starcache {

const static uint32_t kHashMapShardBits = 6;

class HashTableAccessIndex : public AccessIndex {
public:
    bool insert(const CacheId& id, const CacheItemPtr& item) override;
    CacheItemPtr find(const CacheId& id) override;
    bool remove(const CacheId& id) override;

private:
    ParallelHashMap<CacheId, CacheItemPtr, kHashMapShardBits> _cache_items;
};

} // namespace starrocks::starcache
