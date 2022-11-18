// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "storage/compaction_policy.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

class CompactionPolicy;

struct CompactionContext {
    int64_t score = 0;
    CompactionType type = INVALID_COMPACTION;
    std::unique_ptr<CompactionPolicy> policy;
};

} // namespace starrocks
