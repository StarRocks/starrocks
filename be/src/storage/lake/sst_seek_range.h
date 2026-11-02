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

#include "base/string/slice.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/sstable/comparator.h"

namespace starrocks::lake {

// SstSeekRange represents a basic key range unit for compaction tasks split.
// The scan range is [seek_key, stop_key). stop_key is exclusive.
class SstSeekRange {
public:
    // could be empty meaning infinity
    std::string seek_key;
    std::string stop_key;

    SstSeekRange() = default;
    SstSeekRange(std::string seek, std::string stop) : seek_key(std::move(seek)), stop_key(std::move(stop)) {}

    // Return true when [seek_key, stop_key) and [range.start_key(), range.end_key()] overlap.
    bool has_overlap(const PersistentIndexSstableRangePB& range) const;

    // Return true when [seek_key, stop_key) fully contains [range.start_key(), range.end_key()].
    bool full_contains(const PersistentIndexSstableRangePB& range) const;

    // Return true when key falls in [seek_key, stop_key); an empty bound is unbounded on that side.
    bool contains(const Slice& key) const;

    // Intersect this range with rhs.
    SstSeekRange& operator&=(const SstSeekRange& rhs);
};

// Return true when sst declares both a start key and an end key. An empty key is a valid encoded key, so the check is
// on presence, not on emptiness.
bool sstable_has_usable_range(const PersistentIndexSstablePB& sst);

// Return true when a tablet owning tablet_range cannot keep sst as is: sst is shared with a split sibling, declares no
// usable key range, or reaches outside tablet_range. A null tablet_range skips the last check.
bool sstable_needs_rewrite(const PersistentIndexSstablePB& sst, const SstSeekRange* tablet_range);

} // namespace starrocks::lake
