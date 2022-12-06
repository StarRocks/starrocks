// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/capacity_based_promotion_policy.h"

#include "star_cache/mem_space_manager.h"

namespace starrocks {

CapacityBasedPromotionPolicy::CapacityBasedPromotionPolicy(const Config& config)
    : _mem_cap_threshold(config.mem_cap_threshold) {}

BlockLocation CapacityBasedPromotionPolicy::check_write(const CacheItemPtr& cache_item, const BlockKey& block_key) {
    auto mem_space_mgr = MemSpaceManager::GetInstance();
    double used_rate = static_cast<double>(mem_space_mgr->used_bytes()) / mem_space_mgr->quota_bytes();
    if (used_rate < _mem_cap_threshold && !_is_mem_overload()) {
        return BlockLocation::MEM;
    }
    if (!_is_disk_overload()) {
        return BlockLocation::DISK;
    }
    return BlockLocation::NONE;
}

bool CapacityBasedPromotionPolicy::check_promote(const CacheItemPtr& cache_item, const BlockKey& block_key) {
    if (UNLIKELY(_is_mem_overload())) {
        return true;
    }
    return false;
}

} // namespace starrocks
