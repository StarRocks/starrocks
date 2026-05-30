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

#include <unordered_map>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/variant_tuple.h"

namespace starrocks {
class TabletRange;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;

// Segment-level information for range split calculation.
struct SegmentSplitInfo {
    VariantTuple min_key;
    VariantTuple max_key;
    int64_t num_rows = 0;
    int64_t data_size = 0;
    uint32_t source_id = 0; // Optional: for per-source statistics tracking (e.g., rowset_id)

    // Equal-row-interval samples of the sort key (NON-DECREASING) and the row
    // interval used to produce them. Populated from SegmentMetadataPB when
    // available. Empty sort_key_samples <=> sort_key_sample_row_interval == 0.
    // When non-empty, the producer guarantees
    //   sort_key_samples.size() * sort_key_sample_row_interval < num_rows.
    // Consumed by calculate_range_split_boundaries() to treat each segment as
    // N+1 sub-segments with known row counts, instead of a single [min, max]
    // range with a single divide-by-overlap-count estimate.
    std::vector<VariantTuple> sort_key_samples;
    int64_t sort_key_sample_row_interval = 0;

    // Load sort-key samples from a SegmentMetadataPB. Validates that
    // sort_key_samples.size() * sort_key_sample_row_interval < num_rows
    // (overflow-safe); on failure, leaves sort_key_samples and
    // sort_key_sample_row_interval at their defaults (empty/zero).
    // Requires num_rows to be set before calling.
    Status load_sort_key_samples(const SegmentMetadataPB& segment_meta);
};

// Per-range estimated statistics keyed by source_id.
using SourceStats = std::unordered_map<uint32_t, std::pair<int64_t, int64_t>>; // source_id -> (num_rows, data_size)

// Result of range split boundary calculation.
struct RangeSplitResult {
    // N-1 boundary VariantTuples for N splits.
    std::vector<VariantTuple> boundaries;
    // N estimated data sizes (bytes), one per split range.
    std::vector<int64_t> range_data_sizes;
    // N estimated row counts, one per split range.
    std::vector<int64_t> range_num_rows;
    // N per-source statistics, one per split range. Only populated when track_sources is true.
    std::vector<SourceStats> range_source_stats;
};

// Calculate range split boundaries from segment information.
//
// This is the core algorithm shared by tablet splitting, resharding, and range-split compaction.
// It builds ordered ranges from segment key boundaries, distributes data proportionally across
// overlapping ranges, and then calculates split boundaries using a greedy algorithm.
//
// Parameters:
//   segments: segment-level key bounds with data statistics
//   target_split_count: desired number of splits (>= 2)
//   target_value_per_split: target data size (or row count) per split
//   use_num_rows: if true, use num_rows for balancing; otherwise use data_size
//   track_sources: if true, track per-source (e.g., per-rowset) statistics in the result
//   tablet_range: if non-null, only consider ranges overlapping the tablet range for the
//                 greedy algorithm, and only place boundaries that strictly_contains returns
//                 true for. This filters out-of-range boundaries inside the core algorithm.
//
// Returns RangeSplitResult with boundaries and per-range estimates, or empty boundaries if
// splitting is not possible (e.g., not enough data or segments).
//
// colocate_column_count > 0 enables colocate-aware boundary canonicalization: when the
// selected boundary crosses a colocate-prefix transition between adjacent candidate ranges,
// the boundary tuple is replaced with the canonical (prefix(right), NULL, ..., NULL) shape.
// This lets the FE classify the resulting tablet ranges as Level 1 (new ColocateRange) vs
// Level 2 (intra-colocate) via syntactic equality on the lower bound.
// colocate_column_count == 0 (default) preserves the pre-P3 behavior exactly.
StatusOr<RangeSplitResult> calculate_range_split_boundaries(const std::vector<SegmentSplitInfo>& segments,
                                                            int32_t target_split_count, int64_t target_value_per_split,
                                                            bool use_num_rows, bool track_sources = false,
                                                            const TabletRange* tablet_range = nullptr,
                                                            int32_t colocate_column_count = 0);

StatusOr<std::unordered_map<int64_t, MutableTabletMetadataPtr>> split_tablet(
        TabletManager* tablet_manager, const TabletMetadataPtr& old_tablet_metadata,
        const SplittingTabletInfoPB& splitting_tablet, int64_t new_version, const TxnInfoPB& txn_info);

// Per-rowset estimated stats; used by the external boundaries split path's per-range output.
struct Statistic {
    int64_t num_rows = 0;
    int64_t data_size = 0;
    int64_t num_dels = 0;
};

// A new-tablet range plus the per-rowset stats that should be carried into it.
struct TabletRangeInfo {
    TabletRangePB range;
    std::unordered_map<uint32_t, Statistic> rowset_stats;
};

// Data-driven peer of compute_split_ranges_from_external_boundaries:
// computes K-1 boundaries from the old tablet's segment distribution and
// emits K TabletRangeInfo with per-rowset anchored stats. Exposed for unit
// testing (parity comparison with the external-boundaries path); the production call site
// is in split_tablet().
//
// `tablet_manager` is only dereferenced by build_rowset_anchor's primary-key
// delvec fallback. For tests using DUP_KEYS metadata with num_dels populated
// directly, `tablet_manager` may be nullptr.
Status get_tablet_split_ranges(TabletManager* tablet_manager, const TabletMetadataPtr& tablet_metadata,
                               int32_t split_count, std::vector<TabletRangeInfo>* split_ranges,
                               int32_t colocate_column_count = 0);

// external-boundaries peer of get_tablet_split_ranges: produces a vector<TabletRangeInfo>
// from FE-supplied boundaries instead of computing them from segment
// distribution. Exposed for unit testing of the validation paths; the
// production call site is in split_tablet().
//
// `tablet_manager` is only dereferenced when the old tablet has rowsets
// (build_rowset_anchor at step 9). For empty-tablet validation tests
// `tablet_manager` may be nullptr.
Status compute_split_ranges_from_external_boundaries(
        TabletManager* tablet_manager, const TabletMetadataPtr& old_tablet_metadata,
        const google::protobuf::RepeatedPtrField<TabletRangePB>& external_ranges,
        std::vector<TabletRangeInfo>* split_ranges);

} // namespace starrocks::lake
