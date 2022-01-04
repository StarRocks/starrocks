// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <memory>
#include <set>
#include <vector>

#include "storage/rowset/rowset.h"

namespace starrocks {

class Tablet;
class RowsetReleaseGuard;

#ifndef LEVEL_NUMBER
#define LEVEL_NUMBER 3
#endif

struct RowsetComparator {
    bool operator()(const Rowset* left, const Rowset* right) const {
        return left->start_version() < right->start_version() && left->end_version() < right->start_version();
    }
};
struct CompactionContext {
    // sort rowsets by version
    std::set<Rowset*, RowsetComparator> rowset_levels[LEVEL_NUMBER];
    double compaction_scores[LEVEL_NUMBER - 1];
    Tablet* tablet;
    int8_t current_level = -1;

    double get_score() {
        if (current_level < 0 || current_level > 1) {
            return 0;
        }
        return compaction_scores[current_level];
    }

    std::string to_string() const;
};

} // namespace starrocks
