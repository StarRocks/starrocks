// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/range.h"
#include "storage/rowset/bitmap_range_iterator.h"

namespace starrocks::vectorized {

static inline SparseRange roaring2range(const Roaring& roaring) {
    SparseRange range;
    BitmapRangeIterator iter(roaring);
    uint32_t from;
    uint32_t to;
    while (iter.next_range(1024, &from, &to)) {
        range.add(Range(from, to));
    }
    return range;
}

static inline Roaring range2roaring(const SparseRange& range) {
    Roaring roaring;
    for (SparseRangeIterator iter = range.new_iterator(); iter.has_more(); /**/) {
        Range r = iter.next(1000000);
        roaring.addRange(r.begin(), r.end());
    }
    return roaring;
}

} // namespace starrocks::vectorized
