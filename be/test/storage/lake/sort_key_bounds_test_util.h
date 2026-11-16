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

#include <cstddef>

#include "common/logging.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/sort_key_bounds.h"
#include "storage/variant_tuple.h"
#include "testutil/assert.h"

namespace starrocks {

// How a fixture stores the sort key bounds of its segments.
enum class SortKeyBoundsRepr {
    // The deprecated TuplePB pair, as binaries before the encoded form wrote them.
    kDeprecated,
    // The encoded pair, as the current writer stores them.
    kEncoded,
    // Both forms side by side, as in a tablet written across an upgrade.
    kMixed,
};

namespace sort_key_bounds_test_util_internal {

inline void convert_segment(SegmentMetadataPB* segment, SortKeyBoundsRepr repr, size_t* deprecated_segments) {
    if (!segment->has_deprecated_sort_key_min() && !segment->has_deprecated_sort_key_max()) return;
    const bool convert =
            repr == SortKeyBoundsRepr::kEncoded || (repr == SortKeyBoundsRepr::kMixed && *deprecated_segments % 2 == 0);
    ++*deprecated_segments;
    if (!convert) return;
    VariantTuple min;
    VariantTuple max;
    CHECK_OK(decode_segment_sort_key_bounds(*segment, &min, &max));
    set_segment_sort_key_bounds(min, max, segment);
    // A fixture whose bounds silently stayed deprecated would make every parity check vacuous.
    CHECK(min.empty() || segment->has_sort_key_min_encoded());
}

inline void convert_rowset(RowsetMetadataPB* rowset, SortKeyBoundsRepr repr, size_t* deprecated_segments) {
    for (auto& segment : *rowset->mutable_segment_metas()) {
        convert_segment(&segment, repr, deprecated_segments);
    }
}

} // namespace sort_key_bounds_test_util_internal

// Rewrites the segments that carry the deprecated pair into the representation `repr`: kDeprecated leaves them,
// kEncoded rewrites every one of them into the encoded pair, and kMixed rewrites the first, third, fifth... of them in
// metadata order. A rewritten segment whose deprecated pair held no bounds ends up with no bound fields at all, as the
// current writer leaves such a segment. Segments without the deprecated pair are left alone.
inline void convert_sort_key_bounds(RowsetMetadataPB* rowset, SortKeyBoundsRepr repr) {
    size_t deprecated_segments = 0;
    sort_key_bounds_test_util_internal::convert_rowset(rowset, repr, &deprecated_segments);
}

// Same as above over every rowset of the tablet; kMixed keeps alternating across rowset boundaries.
inline void convert_sort_key_bounds(TabletMetadataPB* metadata, SortKeyBoundsRepr repr) {
    size_t deprecated_segments = 0;
    for (auto& rowset : *metadata->mutable_rowsets()) {
        sort_key_bounds_test_util_internal::convert_rowset(&rowset, repr, &deprecated_segments);
    }
}

// Rewrites every segment's bounds into the deprecated pair exactly as VariantTuple::to_proto writes it, and clears
// the bound fields of a segment without bounds, so that metadata derived from sources in different representations
// can be compared byte for byte.
inline void normalize_sort_key_bounds(TabletMetadataPB* metadata) {
    for (auto& rowset : *metadata->mutable_rowsets()) {
        for (auto& segment : *rowset.mutable_segment_metas()) {
            VariantTuple min;
            VariantTuple max;
            CHECK_OK(decode_segment_sort_key_bounds(segment, &min, &max));
            segment.clear_deprecated_sort_key_min();
            segment.clear_deprecated_sort_key_max();
            segment.clear_sort_key_min_encoded();
            segment.clear_sort_key_max_encoded();
            if (!min.empty() && !max.empty()) {
                min.to_proto(segment.mutable_deprecated_sort_key_min());
                max.to_proto(segment.mutable_deprecated_sort_key_max());
            }
        }
    }
}

} // namespace starrocks
