// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "storage/olap_common.h"
#include "storage/tablet.h"

namespace starrocks {

struct CompactionCandidate {
    TabletSharedPtr tablet;
    CompactionType type;
    double score = 0;

    CompactionCandidate() : tablet(nullptr), type(INVALID_COMPACTION) {}

    CompactionCandidate(TabletSharedPtr t, CompactionType compaction_type)
            : tablet(std::move(t)), type(compaction_type) {}

    CompactionCandidate(const TabletSharedPtr& t, CompactionType compaction_type) : tablet(t), type(compaction_type) {}

    CompactionCandidate(const CompactionCandidate& other) {
        tablet = other.tablet;
        type = other.type;
        score = other.score;
    }

    CompactionCandidate& operator=(const CompactionCandidate& rhs) {
        tablet = rhs.tablet;
        type = rhs.type;
        score = rhs.score;
        return *this;
    }

    CompactionCandidate(CompactionCandidate&& other) {
        tablet = std::move(other.tablet);
        type = other.type;
        score = other.score;
    }

    CompactionCandidate& operator=(CompactionCandidate&& rhs) {
        tablet = std::move(rhs.tablet);
        type = rhs.type;
        score = rhs.score;
        return *this;
    }

    std::string to_string() const {
        std::stringstream ss;
        if (tablet) {
            ss << "tablet_id:" << tablet->tablet_id();
        } else {
            ss << "nullptr tablet";
        }
        ss << ", type:" << starrocks::to_string(type);
        ss << ", score:" << score;
        return ss.str();
    }
};

// Comparator should compare tablet by compaction score in descending order
// When compaction scores are equal, put smaller level ahead
// when compaction score and level are equal, use tablet id(to be unique) instead(ascending)
struct CompactionCandidateComparator {
    bool operator()(const CompactionCandidate& left, const CompactionCandidate& right) const {
        return left.score > right.score || (left.score == right.score && left.type > right.type) ||
               (left.score == right.score && left.type == right.type &&
                left.tablet->tablet_id() < right.tablet->tablet_id());
    }
};

} // namespace starrocks
