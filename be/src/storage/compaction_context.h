// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <memory>
#include <set>
#include <vector>

#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

class RowsetReleaseGuard;
class CompactionTask;

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
    double compaction_scores[LEVEL_NUMBER - 1];
    TabletSharedPtr tablet;
    int8_t current_level = -1;

    bool need_compaction(uint8_t level) { return compaction_scores[level] > COMPACTION_SCORE_THRESHOLD; }

    std::string to_string() const;
};

} // namespace starrocks
