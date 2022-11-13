// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <shared_mutex>
#include "star_cache/block_item.h"

namespace starrocks {

enum CacheState {
    IN_MEM,
    IN_DISK,
    RELEASED
};

struct CacheItem {
    // An unique id generated internally
    // CacheId cache_id;
    // A key string passed by api
    std::string cache_key;
    // The block lists belong to this cache item
    BlockItem* blocks;
    // The block count of this cache item
    uint32_t block_count;
    // The cache object size
    size_t size;
    // The expire time of this cache item
    uint64_t expire_time;

    CacheItem() {}
    CacheItem(const std::string& cache_key_, uint32_t block_count_, size_t size_, uint64_t expire_time_)
        : cache_key(cache_key_)
        , block_count(block_count_)
        , size(size_)
        , expire_time(expire_time_)
        , _state(0) {
        blocks = new BlockItem[block_count];
    }
    ~CacheItem() {
        delete[] blocks;
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

    bool set_mem_block_item(BlockItem& block, MemBlockItem* mem_block) {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        if (mem_block) {
            if (_state & (1ul << RELEASED)) {
                return false;
            }
            _state |= (1ul << IN_MEM);
        }
        block.mem_block_item = mem_block;
        return true;
    }

    bool set_disk_block_item(BlockItem& block, DiskBlockItem* disk_block) {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        if (disk_block) {
            if (_state & (1ul << RELEASED)) {
                return false;
            }
            _state |= (1ul << IN_DISK);
        }
        block.disk_block_item = disk_block;
        return true;
    }

    bool release() {
        std::unique_lock<std::shared_mutex> write_lock(_mutex);
        // if ((_state & (1ul << IN_MEM)) || (_state & (1ul << IN_MEM))) {
        if (_state & (1ul << IN_MEM)) {
            return false;
        }
        _state |= (1ul << RELEASED);
        return true;
    }

private:
    // Indate current state
    uint8_t _state;
    std::shared_mutex _mutex;
};

using CacheItemPtr = std::shared_ptr<CacheItem>;

} // namespace starrocks
