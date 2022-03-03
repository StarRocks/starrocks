// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <memory>
#include <set>
#include <vector>

#include "storage/tablet.h"

namespace starrocks {

struct CompactionCandidate {
    TabletSharedPtr tablet;
    int8_t level;

    CompactionCandidate() : tablet(nullptr), level(-1) {}

    CompactionCandidate(TabletSharedPtr t, int8_t l) : tablet(std::move(t)), level(l) {}

    CompactionCandidate(const TabletSharedPtr& t, int8_t l) : tablet(t), level(l) {}

    CompactionCandidate(const CompactionCandidate& other) {
        tablet = other.tablet;
        level = other.level;
    }

    CompactionCandidate& operator=(const CompactionCandidate& rhs) {
        tablet = rhs.tablet;
        level = rhs.level;
        return *this;
    }

    CompactionCandidate(CompactionCandidate&& other) {
        tablet = std::move(other.tablet);
        level = other.level;
    }

    CompactionCandidate& operator=(CompactionCandidate&& rhs) {
        tablet = std::move(rhs.tablet);
        level = rhs.level;
        return *this;
    }

    bool is_valid() { return tablet && level >= 0 && level <= 1; }

    std::string to_string() const {
        std::stringstream ss;
        if (tablet) {
            ss << "tablet_id:" << tablet->tablet_id();
        } else {
            ss << "nullptr tablet";
        }
        ss << ", level:" << (int32_t)level;
        return ss.str();
    }
};

// Comparator should compare tablet by compaction score in descending order
// When compaction scores are equal, put smaller level ahead
// when compaction score and level are equal, use tablet id(to be unique) instead(ascending)
struct CompactionCandidateComparator {
    bool operator()(const CompactionCandidate& left, const CompactionCandidate& right) const {
        int64_t left_score = static_cast<int64_t>(left.tablet->compaction_score(left.level) * 100);
        int64_t right_score = static_cast<int64_t>(right.tablet->compaction_score(right.level) * 100);
        return left_score > right_score || (left_score == right_score && left.level < right.level) ||
               (left_score == right_score && left.level == right.level &&
                left.tablet->tablet_id() < right.tablet->tablet_id());
    }
};

} // namespace starrocks
