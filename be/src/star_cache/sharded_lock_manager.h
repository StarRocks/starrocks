// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <shared_mutex>
#include <butil/memory/singleton.h>

#include "star_cache/common/config.h"

namespace starrocks::starcache {

class ShardedLockManager {
public:
    static ShardedLockManager* GetInstance() {
        return Singleton<ShardedLockManager>::get();
    }

    std::unique_lock<std::shared_mutex> unique_lock(uint64_t hash) {
        std::unique_lock<std::shared_mutex> lck(_mutexes[hash % _mutexes.size()], std::defer_lock);
        return lck;
    }

    std::shared_lock<std::shared_mutex> shared_lock(uint64_t hash) {
        std::shared_lock<std::shared_mutex> lck(_mutexes[hash % _mutexes.size()], std::defer_lock);
        return lck;
    }

private:
    ShardedLockManager() : _mutexes(1lu << config::FLAGS_sharded_lock_shard_bits) {}

    friend struct DefaultSingletonTraits<ShardedLockManager>;
    DISALLOW_COPY_AND_ASSIGN(ShardedLockManager);

    std::vector<std::shared_mutex> _mutexes;
};

inline std::unique_lock<std::shared_mutex> block_unique_lock(const BlockKey& key) {
    return ShardedLockManager::GetInstance()->unique_lock(block_shard(key));
}

inline std::shared_lock<std::shared_mutex> block_shared_lock(const BlockKey& key) {
    return ShardedLockManager::GetInstance()->shared_lock(block_shard(key));
}

} // namespace starrocks::starcache
