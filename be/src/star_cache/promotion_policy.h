// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "star_cache/cache_item.h"

namespace starrocks {

enum class BlockLocation: uint8_t {
    NONE,
    MEM,
    DISK
};

class BlockKey;
// The class to decide the block location
class PromotionPolicy {
public:
    virtual ~PromotionPolicy() = default;

	// Check the location to write the given block first time
	virtual BlockLocation check_write(const CacheItemPtr& cache_item, const BlockKey& block_key) = 0;

	// Check whether to promote the given block
	virtual bool check_promote(const CacheItemPtr& cache_item, const BlockKey& block_key) = 0;
};

} // namespace starrocks
