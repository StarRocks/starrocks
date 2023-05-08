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

#include "storage/olap_common.h"
#include "storage/range.h"

namespace starrocks {

class Rowset;
class Segment;

// It represents a specific rowid range on the segment with `segment_id` of the rowset with `rowset_id`.
struct RowidRangeOption {
public:
    RowidRangeOption() = default;

    void add(const Rowset* rowset, const Segment* segment, SparseRangePtr rowid_range);

    bool match_rowset(const Rowset* rowset) const;
    SparseRangePtr get_segment_rowid_range(const Rowset* rowset, const Segment* segment);

public:
    using SetgmentRowidRangeMap = std::unordered_map<uint64_t, SparseRangePtr>;
    using RowsetRowidRangeMap = std::map<RowsetId, SetgmentRowidRangeMap>;

    RowsetRowidRangeMap rowid_range_per_segment_per_rowset;
};

} // namespace starrocks
