// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "star_cache/cache_item.h"

namespace starrocks::starcache {

enum class BlockLocation: uint8_t {
    NONE,
    MEM,
    DISK
};

inline std::ostream& operator<<(std::ostream& os, const BlockLocation& loc) {
    switch (loc) {
        case BlockLocation::MEM:
            os << "memory";
            break;
        case BlockLocation::DISK:
            os << "disk";
            break;
        default:
            os << "none";
    }
    return os;
}

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

} // namespace starrocks::starcache
