// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "common/status.h"
#include "star_cache/cache_item.h"
#include "star_cache/eviction_policy.h"

namespace starrocks::starcache {

struct MemCacheOptions {
    // Cache Space (Required)
    uint64_t mem_quota_bytes;
    
    // Policy (Optional)
    /*
    EvictPolicy evict_policy;
    */
};

class MemSpaceManager;

class MemCache {
public:
    MemCache() {}
    ~MemCache() {
        delete _eviction_policy;
    }

	Status init(const MemCacheOptions& options);

    // Return old segments instead of freeing them directly because they can be freed without locks
    Status write_block(const BlockKey& key, MemBlockPtr block, const std::vector<BlockSegmentPtr>& segments) const;
    Status read_block(const BlockKey& key, MemBlockPtr block, off_t offset, size_t size,
                      std::vector<BlockSegment>* segments) const;

    CacheItemPtr new_cache_item(const CacheKey& cache_key, size_t size, uint64_t expire_time, bool urgent) const;
    MemBlockPtr new_block_item(const BlockKey& key, BlockState state, bool urgent) const;
    BlockSegmentPtr new_block_segment(off_t offset, uint32_t size, const IOBuf& buf, bool urgent) const;
    void set_block_segment(MemBlockPtr block, int start_slice_index, int end_slice_index,
                           BlockSegmentPtr segment) const;

    void evict_track(const BlockKey& key, size_t size) const;
    void evict_untrack(const BlockKey& key) const;
    EvictionPolicy<BlockKey>::HandlePtr evict_touch(const BlockKey& key) const;
    void evict_for(const BlockKey& key, size_t count, std::vector<BlockKey>* evicted) const;
    void evict(size_t count, std::vector<BlockKey>* evicted) const;

    static size_t block_size(CacheItemPtr cache_item, int block_index) {
        int64_t size = cache_item->size - block_index * config::FLAGS_block_size;
        DCHECK(size >= 0);
        if (size < config::FLAGS_block_size) {
            return size;
        }
        return config::FLAGS_block_size;
    }

private:
    MemSpaceManager* _space_manager = nullptr;
    EvictionPolicy<BlockKey>* _eviction_policy = nullptr;
};

} // namespace starrocks::starcache
