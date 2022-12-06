// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "star_cache/promotion_policy.h"

namespace starrocks {

// The class to decide the block location based on the memory capacity
class CapacityBasedPromotionPolicy : public PromotionPolicy {
public:
    struct Config {
        double mem_cap_threshold;
    };
    explicit CapacityBasedPromotionPolicy(const Config& config);
    ~CapacityBasedPromotionPolicy() = default;

	BlockLocation check_write(const CacheItemPtr& cache_item, const BlockKey& block_key) override;

	bool check_promote(const CacheItemPtr& cache_item, const BlockKey& block_key) override;

private:
    // TODO: Monitor disk ioutil or io latency to reject some write requets when disks overload.
    bool _is_disk_overload() const {
        return false;
    }

    // TODO: Monitor memory allocation/free state to reject some promotion when massive memory operation
    // are pending.
    bool _is_mem_overload() const {
        return false;
    }

    double _mem_cap_threshold;
};

} // namespace starrocks
