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

#include <cstdint>
#include <memory>
#include <vector>

#include "storage/seek_range.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/olap_scan_range.h"

namespace starrocks {

// True only when `query` and `tablet_range` provably share no key, so the caller may skip the query
// range entirely.
//
// Both ranges are compared in the segment's sort-key space by encoding each bound to
// `num_sort_key_columns` with SeekTuple::full_sort_key_encode(), which is order preserving. A bound
// shorter than the full sort key is padded with KEY_MINIMAL_MARKER as a lower bound and
// KEY_MAXIMAL_MARKER as an upper bound, so a prefix bound covers exactly the keys it should and the
// differing-arity case needs no special handling. That mirrors how SegmentIterator builds its own
// search keys.
//
// Returns false whenever disjointness cannot be established -- an unbounded side, or an encoding that
// cannot be produced. Never the other way round: a wrong "disjoint" silently drops rows.
bool seek_range_disjoint_from_tablet_range(const SeekRange& query, const SeekRange& tablet_range,
                                           size_t num_sort_key_columns);

// Distribution topology of one tablet, translated from TInternalScanRange.scan_key_constraint by the
// scan layer. Deliberately thrift-free so the storage layer keeps no dependency on plan types and the
// pruner stays unit-testable on its own.
struct TabletHashBucketConstraint {
    // Positions of the distribution columns inside the scan-key tuple, in DDL declaration order.
    // The tuple is indexed by sort-key position, matching TabletReader's OlapTuple -> SeekTuple
    // conversion.
    std::vector<int32_t> distribution_key_positions;
    // Ordinal of this tablet within its physical partition's full tablet list.
    int32_t bucket_id = -1;
    // Bucket count of that physical partition -- not the number of tablets the query selected.
    int32_t bucket_num = 0;
    int32_t hash_version = 1;
    // True when FE's distribution pruning actually narrowed the scan. Advisory only: it tells the
    // caller whether an empty result is impossible-by-construction (and therefore evidence of a
    // broken hash contract) or simply a tablet FE could not rule out. Never affects what is pruned.
    bool pruning_was_exact = false;
};

// Outcome of dropping the scan keys that cannot live on one tablet.
struct TabletScanKeyPruneResult {
    // Ranges to hand to the reader. Borrowed from the caller's owning container, which must outlive
    // this result.
    std::vector<OlapScanRange*> ranges;

    // Every original range was proven irrelevant to this tablet.
    //
    // The caller MUST NOT pass an empty range vector down to the storage layer:
    // SegmentIterator::_get_row_ranges_by_key_ranges() reads an empty `_opts.ranges` as "no
    // scan-key predicate at all" and returns the whole segment. Skip the tablet instead.
    bool exact_empty = false;

    // The constraint could not be applied, so `ranges` holds every original range. Not an error --
    // an unsupported type, a malformed topology or an unknown hash version all land here.
    bool fallback = false;

    // How many ranges were dropped. 0 with fallback=false simply means nothing was prunable.
    int64_t pruned = 0;
};

// Drops scan keys that provably belong to a different tablet.
//
// Reads only the distribution topology; the values come from the scan keys the BE already built.
// Never touches the predicate tree, never reads segment data.
class TabletScanKeyPruner {
public:
    // Routes each range by hashing its distribution-column values with the same
    // Column::crc32_hash() the load path uses, then comparing against this tablet's bucket.
    //
    // A range is kept whenever routing cannot be proven: a distribution column that is not a single
    // fixed non-null value, an out-of-range position, an unsupported type, or a hash version this
    // build does not implement. Keeping is always safe; dropping is what must be justified.
    static TabletScanKeyPruneResult prune_hash(const TabletHashBucketConstraint& constraint,
                                               const TabletSchema& tablet_schema,
                                               const std::vector<OlapScanRange*>& ranges);

    // Convenience overload for callers holding the owning vector (local OLAP scan).
    static TabletScanKeyPruneResult prune_hash(const TabletHashBucketConstraint& constraint,
                                               const TabletSchema& tablet_schema,
                                               const std::vector<std::unique_ptr<OlapScanRange>>& ranges);

    // Highest hash contract this build understands. FE sends its own version; a mismatch falls back
    // for the whole tablet instead of guessing.
    static constexpr int32_t kSupportedHashVersion = 1;

    // Whether a distribution column of this type can be routed. Restricted to the types whose
    // storage-layer column representation is known to hash identically to the execution-layer column
    // the load path feeds to crc32_hash(). DECIMAL* stay out until the FE/BE/load type-matrix test
    // covers them, because their column carries precision/scale that a bare LogicalType loses.
    static bool is_routable_type(LogicalType type);
};

} // namespace starrocks
