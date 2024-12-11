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

#include "storage/rowset/rowid_range_option.h"

#include <utility>

<<<<<<< HEAD
#include "storage/rowset/rowset.h"
=======
#include "storage/rowset/base_rowset.h"
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#include "storage/rowset/segment.h"

namespace starrocks {

<<<<<<< HEAD
void RowidRangeOption::add(const Rowset* rowset, const Segment* segment, SparseRangePtr rowid_range) {
=======
void RowidRangeOption::add(const BaseRowset* rowset, const Segment* segment, SparseRangePtr rowid_range,
                           bool is_first_split_of_segment) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    auto rowset_it = rowid_range_per_segment_per_rowset.find(rowset->rowset_id());
    if (rowset_it == rowid_range_per_segment_per_rowset.end()) {
        rowset_it = rowid_range_per_segment_per_rowset.emplace(rowset->rowset_id(), SetgmentRowidRangeMap()).first;
    }

    auto& segment_map = rowset_it->second;
<<<<<<< HEAD
    segment_map.emplace(segment->id(), std::move(rowid_range));
}

bool RowidRangeOption::match_rowset(const Rowset* rowset) const {
    return rowid_range_per_segment_per_rowset.find(rowset->rowset_id()) != rowid_range_per_segment_per_rowset.end();
}

SparseRangePtr RowidRangeOption::get_segment_rowid_range(const Rowset* rowset, const Segment* segment) {
    auto rowset_it = rowid_range_per_segment_per_rowset.find(rowset->rowset_id());
    if (rowset_it == rowid_range_per_segment_per_rowset.end()) {
        return nullptr;
=======
    segment_map.emplace(segment->id(), SegmentSplit{std::move(rowid_range), is_first_split_of_segment});
}

bool RowidRangeOption::contains_rowset(const BaseRowset* rowset) const {
    return rowid_range_per_segment_per_rowset.find(rowset->rowset_id()) != rowid_range_per_segment_per_rowset.end();
}

RowidRangeOption::SegmentSplit RowidRangeOption::get_segment_rowid_range(const BaseRowset* rowset,
                                                                         const Segment* segment) {
    auto rowset_it = rowid_range_per_segment_per_rowset.find(rowset->rowset_id());
    if (rowset_it == rowid_range_per_segment_per_rowset.end()) {
        return {nullptr, false};
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    auto& segment_map = rowset_it->second;
    auto segment_it = segment_map.find(segment->id());
    if (segment_it == segment_map.end()) {
<<<<<<< HEAD
        return nullptr;
=======
        return {nullptr, false};
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
    return segment_it->second;
}

} // namespace starrocks
