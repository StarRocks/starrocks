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
    Status write_block(const BlockKey& key, MemBlockPtr block,
                       const std::vector<BlockSegment*>& segments, std::vector<BlockSegment*>* old_segments) const;
    Status read_block(const BlockKey& key, MemBlockPtr block, off_t offset, size_t size,
                      std::vector<BlockSegment>* segments) const;

    CacheItemPtr new_cache_item(const std::string& cache_key, uint32_t block_count, size_t size,
                                uint64_t expire_time);
    MemBlockPtr new_block_item(const BlockKey& key, BlockState state) const;
    BlockSegment* new_block_segment(off_t offset, const IOBuf& buf) const;
    void set_block_segment(MemBlockPtr block, int start_slice_index, int end_slice_index,
                           BlockSegment* segment) const;
    void free_block_segment(BlockSegment* segment) const;
    void free_block_segments(const std::vector<BlockSegment*> segments) const;

    void evict_track(const BlockKey& key) const;
    void evict_untrack(const BlockKey& key) const;
    EvictionPolicy<BlockKey>::HandlePtr evict_touch(const BlockKey& key) const;
    void evict_for(const BlockKey& key, size_t count, std::vector<BlockKey>* evicted) const;
    void evict(size_t count, std::vector<BlockKey>* evicted) const;

private:
    MemSpaceManager* _space_manager = nullptr;
    EvictionPolicy<BlockKey>* _eviction_policy = nullptr;
};

} // namespace starrocks::starcache
