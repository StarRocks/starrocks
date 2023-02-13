// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <shared_mutex>
#include "star_cache/block_item.h"

namespace starrocks::starcache {

enum CacheState {
    IN_MEM,
    IN_DISK,
    RELEASED
};

struct CacheItem {
    // An unique id generated internally
    // CacheId cache_id;
    // A key string passed by api
    CacheKey cache_key;
    // The block lists belong to this cache item
    BlockItem* blocks;
    // The cache object size
    size_t size;
    // The expire time of this cache item
    uint64_t expire_time;

    CacheItem() {}
    CacheItem(const CacheKey& cache_key_, size_t size_, uint64_t expire_time_)
        : cache_key(cache_key_)
        , size(size_)
        , expire_time(expire_time_)
        , _state(0) {
        blocks = new BlockItem[block_count()];
    }
    ~CacheItem() {
        delete[] blocks;
    }

    size_t block_count() {
        if (size == 0) {
            return 0;
        }
        return (size - 1) / config::FLAGS_block_size + 1;
    }

    size_t block_size(uint32_t block_index) {
        int64_t tail_size = size - block_index * config::FLAGS_block_size;
        DCHECK(tail_size >= 0);
        if (tail_size < config::FLAGS_block_size) {
            return tail_size;
        }
        return config::FLAGS_block_size;
    }

    void set_state(const CacheState& state) {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        _state |= (1ul << state);
    }

    void reset_state(const CacheState& state) {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        _state &= ~(1ul << state);
    }

    uint8_t state() {
        std::shared_lock<std::shared_mutex> read_lock(_mutex);
        return _state;
    }

    bool is_in_mem() {
        std::shared_lock<std::shared_mutex> read_lock(_mutex);
        return _state & (1ul << IN_MEM);
    }

    bool is_in_disk() {
        std::shared_lock<std::shared_mutex> read_lock(_mutex);
        return _state & (1ul << IN_DISK);
    }

    bool is_released() {
        std::shared_lock<std::shared_mutex> read_lock(_mutex);
        return _state & (1ul << RELEASED);
    }

    bool release() {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        if (_state & (1ul << RELEASED)) {
            return false;
        }
        _state |= (1ul << RELEASED);
        return true;
    }

private:
    // Indicate current state
    uint8_t _state;
    std::shared_mutex _mutex;
};

using CacheItemPtr = std::shared_ptr<CacheItem>;

} // namespace starrocks::starcache
