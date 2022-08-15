// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "storage/olap_common.h"
#include "storage/tablet.h"

namespace starrocks {

struct CompactionCandidate {
    TabletSharedPtr tablet;
    CompactionType type;

    CompactionCandidate() : tablet(nullptr), type(INVALID_COMPACTION) {}

    CompactionCandidate(TabletSharedPtr t, CompactionType compaction_type)
            : tablet(std::move(t)), type(compaction_type) {}

    CompactionCandidate(const TabletSharedPtr& t, CompactionType compaction_type) : tablet(t), type(compaction_type) {}

    CompactionCandidate(const CompactionCandidate& other) {
        tablet = other.tablet;
        type = other.type;
    }

    CompactionCandidate& operator=(const CompactionCandidate& rhs) {
        tablet = rhs.tablet;
        type = rhs.type;
        return *this;
    }

    CompactionCandidate(CompactionCandidate&& other) {
        tablet = std::move(other.tablet);
        type = other.type;
    }

    CompactionCandidate& operator=(CompactionCandidate&& rhs) {
        tablet = std::move(rhs.tablet);
        type = rhs.type;
        return *this;
    }

    bool is_valid() { return tablet && (type == BASE_COMPACTION || type == CUMULATIVE_COMPACTION); }

    std::string to_string() const {
        std::stringstream ss;
        if (tablet) {
            ss << "tablet_id:" << tablet->tablet_id();
        } else {
            ss << "nullptr tablet";
        }
        ss << ", type:" << type;
        return ss.str();
    }
};

// Comparator should compare tablet by compaction score in descending order
// When compaction scores are equal, put smaller level ahead
// when compaction score and level are equal, use tablet id(to be unique) instead(ascending)
struct CompactionCandidateComparator {
    bool operator()(const CompactionCandidate& left, const CompactionCandidate& right) const {
        int64_t left_score = static_cast<int64_t>(left.tablet->compaction_score(left.type) * 100);
        int64_t right_score = static_cast<int64_t>(right.tablet->compaction_score(right.type) * 100);
        return left_score > right_score || (left_score == right_score && left.type > right.type) ||
               (left_score == right_score && left.type == right.type &&
                left.tablet->tablet_id() < right.tablet->tablet_id());
    }
};

} // namespace starrocks
