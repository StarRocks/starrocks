// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "common/status.h"
#include "star_cache/block_item.h"
#include "star_cache/eviction_policy.h"

namespace starrocks {

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

    Status write_block(const BlockKey& key, MemBlockItem* block,
                       const std::vector<BlockSegment*>& segments) const;
    Status read_block(const BlockKey& key, MemBlockItem* block, off_t offset, size_t size,
                      std::vector<BlockSegment>* segments) const;

    MemBlockItem* new_block_item(const BlockKey& key, BlockState state) const;
    void free_block_item(MemBlockItem* block) const;

    BlockSegment* new_block_segment(off_t offset, const IOBuf& buf) const;
    void set_block_segment(MemBlockItem* block, int start_slice_index, int end_slice_index,
                           BlockSegment* segment) const;
    void free_block_segment(BlockSegment* segment) const;

    void evict_track(const BlockKey& key) const;
    void evict_untrack(const BlockKey& key) const;
    void evict_for(const BlockKey& key, size_t count, std::vector<BlockKey>* evicted) const;

private:
    MemSpaceManager* _space_manager = nullptr;
    EvictionPolicy<BlockKey>* _eviction_policy = nullptr;
};

} // namespace starrocks
