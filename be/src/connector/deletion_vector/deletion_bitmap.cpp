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

#include "deletion_bitmap.h"

#include "column/vectorized_fwd.h"
#include "util/defer_op.h"

namespace starrocks {

StatusOr<bool> DeletionBitmap::fill_filter(uint64_t start, uint64_t end, Filter& filter) {
    if (start >= end) {
        return false;
    }

    roaring64_iterator_t* it = roaring64_iterator_create(_bitmap);
    DeferOp defer([&it] { roaring64_iterator_free(it); });
    if (!roaring64_iterator_move_equalorlarger(it, start)) {
        return false;
    }

    bool has_filter = false;
    std::vector<uint64_t> buf(kBatchSize, 0);
    while (roaring64_iterator_has_value(it) && roaring64_iterator_value(it) < end) {
        uint64_t count = roaring64_iterator_read(it, buf.data(), kBatchSize);
        for (uint64_t i = 0; i < count && buf[i] < end; ++i) {
            filter[buf[i] - start] = 0;
        }

        has_filter = true;
    }

    return has_filter;
}

uint64_t DeletionBitmap::get_range_cardinality(uint64_t start, uint64_t end) const {
    if (start >= end) {
        return 0;
    }
    return roaring64_bitmap_range_cardinality(_bitmap, start, end);
}

uint64_t DeletionBitmap::get_cardinality() const {
    return roaring64_bitmap_get_cardinality(_bitmap);
}

void DeletionBitmap::to_array(std::vector<uint64_t>& array) const {
    roaring64_bitmap_to_uint64_array(_bitmap, array.data());
}

void DeletionBitmap::add_value(uint64_t val) {
    roaring64_bitmap_add(_bitmap, val);
}

} // namespace starrocks