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

#include <string>

#include "gen_cpp/lake_types.pb.h"
#include "storage/sstable/comparator.h"
#include "util/slice.h"

namespace starrocks::lake {

// SstSeekRange represents a basic key range unit for compaction tasks split.
// The scan range is [seek_key, stop_key). stop_key is exclusive.
struct SstSeekRange {
    std::string seek_key;
    std::string stop_key; // could be empty meaning infinity

    // Return true when [seek_key, stop_key) and [range.start_key(), range.end_key()] overlap.
    bool has_overlap(const PersistentIndexSstableRangePB& range) const {
        const auto* cmp = sstable::BytewiseComparator();
        // range is on the right side of [seek_key, stop_key)
        if (!stop_key.empty() && cmp->Compare(Slice(stop_key), Slice(range.start_key())) <= 0) {
            return false;
        }
        // range is on the left side of [seek_key, stop_key)
        if (cmp->Compare(Slice(range.end_key()), Slice(seek_key)) < 0) {
            return false;
        }
        return true;
    }

    // Return true when [seek_key, stop_key) fully contains [range.start_key(), range.end_key()].
    bool full_contains(const PersistentIndexSstableRangePB& range) const {
        const auto* cmp = sstable::BytewiseComparator();
        if (cmp->Compare(Slice(seek_key), Slice(range.start_key())) > 0) {
            return false;
        }
        if (!stop_key.empty() && cmp->Compare(Slice(stop_key), Slice(range.end_key())) <= 0) {
            return false;
        }
        return true;
    }

    // Intersect this range with rhs.
    SstSeekRange& operator&=(const SstSeekRange& rhs) {
        const auto* cmp = sstable::BytewiseComparator();

        if (seek_key.empty()) {
            seek_key = rhs.seek_key;
        } else if (!rhs.seek_key.empty() && cmp->Compare(Slice(seek_key), Slice(rhs.seek_key)) < 0) {
            seek_key = rhs.seek_key;
        }

        if (stop_key.empty()) {
            stop_key = rhs.stop_key;
        } else if (!rhs.stop_key.empty() && cmp->Compare(Slice(stop_key), Slice(rhs.stop_key)) > 0) {
            stop_key = rhs.stop_key;
        }
        return *this;
    }
};

} // namespace starrocks::lake
