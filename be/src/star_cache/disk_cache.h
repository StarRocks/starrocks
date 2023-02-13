// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <butil/iobuf.h>
#include "common/statusor.h"
#include "star_cache/cache_item.h"
#include "star_cache/eviction_policy.h"

namespace starrocks::starcache {

struct DiskCacheOptions {
    // Cache Space (Required)
    std::vector<DirSpace> disk_dir_spaces;
    
    // Policy (Optional)
    /*
    EvictPolicy evict_policy;
    */
};

class DiskSpaceManager;

class DiskCache {
public:
    DiskCache() {}
    ~DiskCache() {
        delete _eviction_policy;
    }

    Status init(const DiskCacheOptions& options);

    Status write_block(const CacheId& cache_id, DiskBlockPtr block,const BlockSegment& segment) const;
    Status read_block(const CacheId& cache_id, DiskBlockPtr block, BlockSegment* segment) const;
    /*
    Status writev_block(const CacheId& cache_id, DiskBlockPtr block, off_t offset_in_block,
                        const std::vector<IOBuf*>& bufv) const;
    Status readv_block(const CacheId& cache_id, DiskBlockPtr block, off_t offset_in_block,
                       const std::vector<size_t> sizev, std::vector<IOBuf*>* bufv) const;
    */

    DiskBlockPtr new_block_item(const CacheId& cache_id) const;

    void evict_track(const CacheId& id, size_t size) const;
    void evict_untrack(const CacheId& id) const;
    EvictionPolicy<CacheId>::HandlePtr evict_touch(const CacheId& id) const;
    void evict_for(const CacheId& id, size_t count, std::vector<CacheId>* evicted) const;
    void evict(size_t count, std::vector<CacheId>* evicted) const;

private:
    void _update_block_checksum(DiskBlockPtr block, const BlockSegment& segment) const;

    bool _check_block_checksum(DiskBlockPtr block, const BlockSegment& segment) const;

    DiskSpaceManager* _space_manager = nullptr;
    EvictionPolicy<CacheId>* _eviction_policy = nullptr;
};

} // namespace starrocks::starcache
