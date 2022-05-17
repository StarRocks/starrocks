// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>

#include "storage/range.h"

namespace starrocks {

class Rowset;
class Segment;

namespace vectorized {

// It represents a specific rowid range on the segment with `segment_id` of the rowset with `rowset_id`.
struct RowidRangeOption {
public:
    RowidRangeOption(const std::string& rowset_id, const uint64_t segment_id, const SparseRange& rowid_range);

    bool match_rowset(const Rowset* rowset) const;
    bool match_segment(const Segment* segment) const;

public:
    const std::string rowset_id;
    const uint64_t segment_id;
    const SparseRange rowid_range;
};

} // namespace vectorized
} // namespace starrocks
