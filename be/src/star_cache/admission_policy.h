// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "star_cache/cache_item.h"

namespace starrocks::starcache {

enum class BlockAdmission : uint8_t {
    FLUSH,
    SKIP,
    DELETE
};

class BlockKey;
// The class to handle admission control logic
class AdmissionPolicy {
public:
    virtual ~AdmissionPolicy() = default;

    // Check the admisstion control for the given block
    virtual BlockAdmission check_admission(const CacheItemPtr& cache_item, const BlockKey& block_key) = 0;
};

} // namespace starrocks::starcache
