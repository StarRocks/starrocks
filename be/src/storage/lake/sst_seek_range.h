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

    // Intersect this range with rhs.
    SstSeekRange& operator&=(const SstSeekRange& rhs);
};

} // namespace starrocks::lake
