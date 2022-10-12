// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/rowset/rowid_range_option.h"

#include "storage/rowset/rowset.h"
#include "storage/rowset/segment.h"

namespace starrocks::vectorized {

RowidRangeOption::RowidRangeOption(const RowsetId& rowset_id, uint64_t segment_id, const SparseRange& rowid_range)
        : rowset_id(rowset_id), segment_id(segment_id), rowid_range(rowid_range) {}

bool RowidRangeOption::match_rowset(const Rowset* rowset) const {
    return rowset->rowset_id() == rowset_id;
}

bool RowidRangeOption::match_segment(const Segment* segment) const {
    return segment->id() == segment_id;
}

} // namespace starrocks::vectorized
