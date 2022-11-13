// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include "common/status.h"
#include "star_cache/types.h"
#include "star_cache/cache_item.h"
#include "star_cache/thread_safe_hash_map.h"
#include "star_cache/mem_cache.h"
#include "star_cache/disk_cache.h"

namespace starrocks {

class AccessIndex;

struct CacheOptions {
    // Cache Space (Required)
    uint64_t mem_quota_bytes;
    std::vector<DirSpace> disk_dir_spaces;
    
    // Policy (Optional)
    /*
    EvictPolicy mem_evict_policy;
    EvictPolicy disk_evict_policy;
    AdmissionCtrlPolicy admission_ctrl_policy;
    PromotionPolicy promotion_policy;
    */
    
    // Other (Optional) 
    // bool checksum;
};

class StarCache {
public:
    StarCache();
    ~StarCache();

	Status init(const CacheOptions& options);

	Status set(const std::string& cache_key, const IOBuf& buf, 
               uint64_t ttl_seconds=0);

	Status get(const std::string& cache_key, IOBuf* buf);
            
	Status read(const std::string& cache_key, off_t offset, size_t size,
                IOBuf* buf);

    Status remove(const std::string& cache_key) { return Status::OK(); }

    Status set_ttl(const std::string& cache_key, uint64_t ttl_seconds) { return Status::OK(); }


    Status pin(const std::string& cache_key) { return Status::OK(); }


    Status unpin(const std::string& cache_key) { return Status::OK(); }

private:
    Status _read_cache_item(const CacheId& cache_id, CacheItemPtr cache_item, off_t offset, size_t size, IOBuf* buf);
    void _free_cache_item(const CacheId& cache_id);

    Status _read_block(const BlockKey& block_key, BlockItem* block, off_t offset, size_t size,
                       IOBuf* buf);
    Status _flush_block(const BlockKey& block_key, BlockItem* block);
    void _promote_block_segments(const BlockKey& block_key, BlockItem* block,
                                 const std::vector<BlockSegment>& segments);
    void _evict_for_mem_block(const BlockKey& block_key);
    void _evict_for_disk_block(const CacheId& cache_id);
    void _clean_disk_cache(CacheItemPtr cache);
    BlockSegment* _alloc_block_segment(const BlockKey& block_key, off_t offset, const IOBuf& buf);
    DiskBlockItem* _alloc_disk_block(const BlockKey& block_key);

    BlockItem* _get_block(const BlockKey& block_key);
    BlockItem* _get_block(const std::string& cache_key, off_t offset);

    std::unique_ptr<MemCache> _mem_cache = nullptr;
    std::unique_ptr<DiskCache> _disk_cache = nullptr;
    AccessIndex* _access_index = nullptr;
};

} // namespace starrocks
