// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

#ifndef LEVEL_NUMBER
#define LEVEL_NUMBER 3
#endif

const double COMPACTION_SCORE_THRESHOLD = 0.999;

// rowsets here can never overlap
struct RowsetComparator {
    bool operator()(const Rowset* left, const Rowset* right) const {
        return left->start_version() < right->start_version();
    }
};

struct CompactionContext {
    // sort rowsets by version
    std::set<Rowset*, RowsetComparator> rowset_levels[LEVEL_NUMBER];
    double cumulative_score = 0;
    double base_score = 0;
    TabletSharedPtr tablet;
    CompactionType chosen_compaction_type = INVALID_COMPACTION;

    bool need_compaction(CompactionType compaction_type) {
        if (compaction_type == BASE_COMPACTION) {
            return base_score > COMPACTION_SCORE_THRESHOLD;
        } else if (compaction_type == CUMULATIVE_COMPACTION) {
            return cumulative_score > COMPACTION_SCORE_THRESHOLD;
        } else {
            return false;
        }
    }

    std::string to_string() const;
};

} // namespace starrocks
