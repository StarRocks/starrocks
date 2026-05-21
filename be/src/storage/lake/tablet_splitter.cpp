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

#include "storage/lake/tablet_splitter.h"

#include <bvar/bvar.h>
#include <fmt/format.h>

#include <algorithm>
#include <unordered_map>
#include <vector>

#include "common/logging.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_range_helper.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_range.h"
#include "storage/tablet_schema.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

extern bvar::Adder<int64_t> g_tablet_reshard_split_fallback_total;
extern bvar::Adder<int64_t> g_tablet_reshard_split_external_boundaries_fallback_total;

namespace starrocks::lake {

using google::protobuf::RepeatedPtrField;

Status SegmentSplitInfo::load_sort_key_samples(const SegmentMetadataPB& segment_meta) {
    if (segment_meta.sort_key_samples_size() == 0 || !segment_meta.has_sort_key_sample_row_interval()) {
        return Status::OK();
    }
    const int64_t row_interval = segment_meta.sort_key_sample_row_interval();
    const int64_t num_samples = segment_meta.sort_key_samples_size();
    // Overflow-safe validity: sort_key_samples.size() * row_interval < num_rows.
    if (!(row_interval > 0 && num_rows > 0 && num_samples <= (num_rows - 1) / row_interval)) {
        return Status::OK(); // Invalid metadata -- fall through with empty samples.
    }
    sort_key_sample_row_interval = row_interval;
    sort_key_samples.reserve(num_samples);
    for (const auto& sample_pb : segment_meta.sort_key_samples()) {
        VariantTuple sample;
        RETURN_IF_ERROR(sample.from_proto(sample_pb));
        DCHECK(sort_key_samples.empty() || sort_key_samples.back().compare(sample) <= 0);
        sort_key_samples.push_back(std::move(sample));
    }
    return Status::OK();
}

// ================================================================================
// Internal data structures and helpers for range splitting
// ================================================================================

namespace {

// Returns true if the first n DatumVariants of `a` and `b` are not all equal.
// Used to detect a colocate-prefix transition between two adjacent candidate
// ranges in the greedy split loop.
bool colocate_prefix_differs(const VariantTuple& a, const VariantTuple& b, int32_t n) {
    DCHECK_GE(static_cast<int32_t>(a.size()), n);
    DCHECK_GE(static_cast<int32_t>(b.size()), n);
    for (int32_t i = 0; i < n; ++i) {
        if (a[i].compare(b[i]) != 0) {
            return true;
        }
    }
    return false;
}

// Returns true iff every position from `colocate_column_count` onward is NULL —
// i.e. `t` has the canonical (k, NULL, ..., NULL) shape that FE produces from a
// colocate-range bound via expandToFullSortKey.
bool is_canonical_tuple(const VariantTuple& t, int32_t colocate_column_count) {
    for (size_t i = colocate_column_count; i < t.size(); ++i) {
        if (!t[i].value().is_null()) {
            return false;
        }
    }
    return true;
}

// Build a canonical colocate boundary tuple by copying the leading
// `colocate_column_count` positions from `source` and NULL-padding the rest. `source`
// supplies both the prefix values and the column types for the trailing positions.
VariantTuple build_canonical_boundary(const VariantTuple& source, int32_t colocate_column_count) {
    DCHECK_GE(static_cast<int32_t>(source.size()), colocate_column_count);
    VariantTuple canonical;
    canonical.reserve(source.size());
    for (int32_t i = 0; i < colocate_column_count; ++i) {
        canonical.append(source[i]);
    }
    Datum null_datum;
    null_datum.set_null();
    for (size_t i = colocate_column_count; i < source.size(); ++i) {
        canonical.emplace(source[i].type(), null_datum);
    }
    return canonical;
}

struct RangeInfo {
    VariantTuple min;
    VariantTuple max;
    int64_t num_rows = 0;
    int64_t data_size = 0;
    SourceStats source_stats;
};

// Find ordered_ranges overlapping [sub_min, sub_max].
// For zero-width sub-segments (sub_min == sub_max), uses [lower, upper) point
// ownership: non-last range owns x iff r.min <= x < r.max; last range owns x
// iff r.min <= x <= r.max. For non-zero-width sub-segments, uses the standard
// two-comparator binary-search overlap rule.
void find_overlapping_ranges(const VariantTuple& sub_min, const VariantTuple& sub_max,
                             std::vector<RangeInfo>& ordered_ranges, std::vector<RangeInfo*>& result) {
    result.clear();
    const size_t last_range_index = ordered_ranges.size() - 1;

    if (sub_min.compare(sub_max) == 0) {
        // Zero-width: point ownership matching split output [lower, upper).
        for (size_t i = 0; i < ordered_ranges.size(); ++i) {
            auto& r = ordered_ranges[i];
            bool owns = (i != last_range_index) ? (r.min.compare(sub_min) <= 0 && sub_min.compare(r.max) < 0)
                                                : (r.min.compare(sub_min) <= 0 && sub_min.compare(r.max) <= 0);
            if (owns) {
                result.push_back(&r);
                return;
            }
        }
        return;
    }

    // Non-zero-width: binary search for the first overlapping range.
    size_t lo = 0, hi = ordered_ranges.size();
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int cmp = ordered_ranges[mid].max.compare(sub_min);
        if ((mid == last_range_index) ? (cmp < 0) : (cmp <= 0)) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    for (size_t i = lo; i < ordered_ranges.size(); i++) {
        auto& r = ordered_ranges[i];
        if (r.min.compare(sub_max) > 0) break;
        if (i != last_range_index && r.min.compare(sub_max) >= 0) break;
        result.push_back(&r);
    }
}

// Distribute num_rows/data_size evenly across overlapping ranges with remainder
// correction, optionally tracking per-source statistics.
void distribute_to_ranges(const std::vector<RangeInfo*>& overlapping, int64_t num_rows, int64_t data_size,
                          uint32_t source_id, bool track_sources) {
    if (overlapping.empty()) return;
    const auto count = static_cast<int64_t>(overlapping.size());
    for (int64_t i = 0; i < count; i++) {
        int64_t delta_rows = num_rows / count + (i < num_rows % count ? 1 : 0);
        int64_t delta_size = data_size / count + (i < data_size % count ? 1 : 0);
        overlapping[i]->num_rows += delta_rows;
        overlapping[i]->data_size += delta_size;
        if (track_sources) {
            auto& stats = overlapping[i]->source_stats[source_id];
            stats.first += delta_rows;
            stats.second += delta_size;
        }
    }
}

// Distribute one segment's data across ordered_ranges. When the segment has
// sort-key samples, it is split into N+1 sub-segments with known row counts;
// otherwise, the entire segment is treated as a single [min_key, max_key] range.
void distribute_segment_to_ranges(const SegmentSplitInfo& segment, std::vector<RangeInfo>& ordered_ranges,
                                  bool track_sources) {
    if (segment.num_rows == 0 && segment.data_size == 0) return;

    std::vector<RangeInfo*> overlapping;
    const int64_t num_samples = static_cast<int64_t>(segment.sort_key_samples.size());

    if (num_samples == 0) {
        find_overlapping_ranges(segment.min_key, segment.max_key, ordered_ranges, overlapping);
        distribute_to_ranges(overlapping, segment.num_rows, segment.data_size, segment.source_id, track_sources);
        return;
    }

    // Sampled path: N+1 sub-segments with known row counts.
    const int64_t row_interval = segment.sort_key_sample_row_interval;
    const int64_t tail_rows = segment.num_rows - num_samples * row_interval;
    DCHECK_GT(tail_rows, 0);

    // bound(k) returns the k-th sub-segment boundary; k in [0, num_samples+1].
    auto bound = [&](int64_t k) -> const VariantTuple& {
        if (k == 0) return segment.min_key;
        if (k == num_samples + 1) return segment.max_key;
        return segment.sort_key_samples[k - 1];
    };
    // Per-sub-segment byte share; 128-bit intermediate avoids signed overflow.
    auto bytes_for = [&](int64_t rows) -> int64_t {
        return static_cast<int64_t>((static_cast<__int128>(rows) * segment.data_size) / segment.num_rows);
    };

    int64_t bytes_assigned = 0;
    for (int64_t k = 0; k <= num_samples; ++k) {
        const int64_t sub_rows = (k < num_samples) ? row_interval : tail_rows;
        DCHECK_GT(sub_rows, 0);
        const int64_t sub_bytes = (k == num_samples) ? (segment.data_size - bytes_assigned) : bytes_for(sub_rows);
        bytes_assigned += sub_bytes;
        find_overlapping_ranges(bound(k), bound(k + 1), ordered_ranges, overlapping);
        distribute_to_ranges(overlapping, sub_rows, sub_bytes, segment.source_id, track_sources);
    }
    DCHECK_EQ(bytes_assigned, segment.data_size);
}

} // anonymous namespace

// ================================================================================
// Core range split algorithm (public API)
// ================================================================================

StatusOr<RangeSplitResult> calculate_range_split_boundaries(const std::vector<SegmentSplitInfo>& segments,
                                                            int32_t target_split_count, int64_t target_value_per_split,
                                                            bool use_num_rows, bool track_sources,
                                                            const TabletRange* tablet_range,
                                                            int32_t colocate_column_count) {
    RangeSplitResult result;

    if (segments.empty() || target_split_count <= 1) {
        return result;
    }

    // Step 1: Collect all unique boundary points (including sort-key samples) and sort them.
    // Owned VariantTuples (not pointers) so step 1b can insert synthesized canonical
    // colocate boundaries that don't exist in any segment.
    std::vector<VariantTuple> ordered_boundary_values;
    for (const auto& segment : segments) {
        ordered_boundary_values.push_back(segment.min_key);
        ordered_boundary_values.push_back(segment.max_key);
        for (const auto& sample : segment.sort_key_samples) {
            ordered_boundary_values.push_back(sample);
        }
    }
    // Insert tablet_range bounds so any ordered_range that previously straddled a
    // tablet_range edge is split exactly at that edge. Without this, a shared
    // segment whose physical key range extends past the tablet's range (a tablet
    // that has been split before still sees the full physical extent of its
    // shared rowsets) can leak its out-of-range data into the per-split
    // estimates: the candidate filter only excludes ranges that don't overlap at
    // all, so a partial-crossing range is kept whole and its full data_size /
    // num_rows is summed into one of the new splits.
    if (tablet_range != nullptr) {
        if (!tablet_range->is_minimum()) {
            ordered_boundary_values.push_back(tablet_range->lower_bound());
        }
        if (!tablet_range->is_maximum()) {
            ordered_boundary_values.push_back(tablet_range->upper_bound());
        }
    }
    std::sort(ordered_boundary_values.begin(), ordered_boundary_values.end());
    ordered_boundary_values.erase(std::unique(ordered_boundary_values.begin(), ordered_boundary_values.end()),
                                  ordered_boundary_values.end());

    // Step 1b: when colocate-aware, synthesize canonical (k, NULL, ..., NULL) tuples at every
    // observed colocate-prefix transition so candidate ranges are pre-split at canonical
    // boundaries. Keeps the post-pass stats walk accurate when the greedy loop picks a
    // canonical boundary.
    if (colocate_column_count > 0 && ordered_boundary_values.size() >= 2) {
        std::vector<VariantTuple> with_canonical;
        with_canonical.reserve(ordered_boundary_values.size() * 2);
        for (size_t i = 0; i < ordered_boundary_values.size(); ++i) {
            if (i > 0 && colocate_prefix_differs(ordered_boundary_values[i - 1], ordered_boundary_values[i],
                                                 colocate_column_count)) {
                VariantTuple canonical = build_canonical_boundary(ordered_boundary_values[i], colocate_column_count);
                // canonical > prev is guaranteed (canonical's prefix matches curr's, which is
                // > prev's prefix when prefix-differs is true). Only the upper guard is real:
                // canonical can equal curr when curr already has the (k, NULL...) shape.
                DCHECK_GT(canonical.compare(ordered_boundary_values[i - 1]), 0);
                if (canonical.compare(ordered_boundary_values[i]) <= 0) {
                    with_canonical.push_back(std::move(canonical));
                }
            }
            with_canonical.push_back(ordered_boundary_values[i]);
        }
        ordered_boundary_values = std::move(with_canonical);
        ordered_boundary_values.erase(std::unique(ordered_boundary_values.begin(), ordered_boundary_values.end()),
                                      ordered_boundary_values.end());
    }

    if (ordered_boundary_values.size() < 2) {
        return result;
    }

    // Step 2: Build ordered ranges between adjacent boundary points.
    std::vector<RangeInfo> ordered_ranges;
    ordered_ranges.reserve(ordered_boundary_values.size());
    for (size_t i = 1; i < ordered_boundary_values.size(); ++i) {
        auto& range_info = ordered_ranges.emplace_back();
        range_info.min = ordered_boundary_values[i - 1];
        range_info.max = ordered_boundary_values[i];
    }

    if (ordered_ranges.empty()) {
        return result;
    }

    // Step 3: Distribute segment data across overlapping ranges.
    for (const auto& segment : segments) {
        distribute_segment_to_ranges(segment, ordered_ranges, track_sources);
    }

    // Step 4: Calculate split boundaries using a greedy algorithm.
    // If tablet_range is provided, only consider ranges that overlap with it.
    //
    // The overlap predicate mirrors find_overlapping_ranges' last-range
    // semantics: every ordered_range except the last is the half-open interval
    // [min, max); the last range is the closed interval [min, max]. The check
    // also has to honor TabletRange's lower / upper inclusion flags (a child
    // produced by split has lower_bound_included=true and upper_bound_included
    // =false, so e.g. a range whose r.max == tablet_range.lower_bound is
    // entirely below the tablet — TabletRange::greater_than alone misses this
    // case because it returns false when the lower bound is included).
    //
    // Precondition assumed by the production caller (split_tablet): tablet
    // ranges follow the [lower, upper) shape — `upper_bound_excluded` is the
    // convention written by `get_tablet_split_ranges` (lower_bound_included
    // = true, upper_bound_included = false; see the per-split range
    // construction below). The corner case `r.min == tablet.upper_bound` with
    // `upper_bound_included` would represent a singleton overlap that the
    // ordered_range model cannot allocate precisely (a non-last
    // ordered_range covers [r.min, r.max), so it would consume more than the
    // single key). For an inclusive upper bound we still mark such an r as
    // overlapping (best-effort), but the caller must rely on the half-open
    // convention to avoid silent over-counting.
    const size_t last_range_index = ordered_ranges.size() - 1;
    auto range_overlaps_tablet = [&](const RangeInfo& r, size_t idx) -> bool {
        if (tablet_range == nullptr) return true;
        if (!tablet_range->is_minimum()) {
            const int cmp = r.max.compare(tablet_range->lower_bound());
            if (idx == last_range_index) {
                // r covers [r.min, r.max]: entirely below iff r.max is strictly
                // less than the smallest key in tablet_range.
                if (cmp < 0) return false;
                if (cmp == 0 && tablet_range->lower_bound_excluded()) return false;
            } else {
                // r covers [r.min, r.max): entirely below iff r.max is at or
                // below the smallest key in tablet_range.
                if (cmp <= 0) return false;
            }
        }
        if (!tablet_range->is_maximum()) {
            const int cmp = r.min.compare(tablet_range->upper_bound());
            if (cmp > 0) return false;
            if (cmp == 0 && tablet_range->upper_bound_excluded()) return false;
        }
        return true;
    };
    std::vector<const RangeInfo*> candidate_ranges;
    candidate_ranges.reserve(ordered_ranges.size());
    for (size_t i = 0; i < ordered_ranges.size(); ++i) {
        if (!range_overlaps_tablet(ordered_ranges[i], i)) {
            continue;
        }
        candidate_ranges.push_back(&ordered_ranges[i]);
    }

    int32_t actual_split_count = std::min(target_split_count, static_cast<int32_t>(candidate_ranges.size()));
    if (actual_split_count <= 1) {
        return result;
    }

    int64_t total_value = 0;
    size_t non_empty_ranges = 0;
    for (const auto* r : candidate_ranges) {
        int64_t val = use_num_rows ? r->num_rows : r->data_size;
        total_value += val;
        if (val > 0) {
            non_empty_ranges++;
        }
    }

    if (non_empty_ranges < static_cast<size_t>(actual_split_count)) {
        return result;
    }

    int64_t actual_target = total_value / actual_split_count;
    if (target_value_per_split > 0) {
        actual_target = std::min(actual_target, target_value_per_split);
    }
    actual_target = std::max<int64_t>(1, actual_target);

    // Pre-compute a suffix count of non-empty ranges starting at each index.
    std::vector<size_t> remaining_non_empty_at(candidate_ranges.size() + 1, 0);
    for (int64_t k = static_cast<int64_t>(candidate_ranges.size()) - 1; k >= 0; k--) {
        int64_t val_k = use_num_rows ? candidate_ranges[k]->num_rows : candidate_ranges[k]->data_size;
        remaining_non_empty_at[k] = remaining_non_empty_at[k + 1] + (val_k > 0 ? 1 : 0);
    }

    std::vector<VariantTuple> boundaries;
    int64_t accumulated = 0;

    for (size_t i = 0; i < candidate_ranges.size(); i++) {
        const auto* range = candidate_ranges[i];
        int64_t val = use_num_rows ? range->num_rows : range->data_size;
        bool is_non_empty = val > 0;

        accumulated += val;

        bool is_last_range = (i == candidate_ranges.size() - 1);
        int32_t remaining_splits = actual_split_count - 1 - static_cast<int32_t>(boundaries.size());
        size_t remaining_non_empty_after = (i + 1 < candidate_ranges.size()) ? remaining_non_empty_at[i + 1] : 0;

        if (!is_last_range && remaining_splits > 0 && is_non_empty &&
            (accumulated >= actual_target || remaining_non_empty_after <= static_cast<size_t>(remaining_splits))) {
            // Advance boundary across trailing empty ranges to maximize natural gaps.
            const VariantTuple* boundary = &range->max;
            for (size_t j = i + 1; j < candidate_ranges.size(); j++) {
                int64_t next_val = use_num_rows ? candidate_ranges[j]->num_rows : candidate_ranges[j]->data_size;
                if (next_val > 0 || j == candidate_ranges.size() - 1) {
                    break;
                }
                if (tablet_range != nullptr && !tablet_range->strictly_contains(candidate_ranges[j]->max)) {
                    break;
                }
                // When colocate-aware, stop the advance once `boundary` is already a canonical
                // colocate boundary placed by step 1b. Walking past it would force FE to
                // classify the split as Level 2 even though the data clearly crosses a
                // prefix transition.
                if (colocate_column_count > 0 && is_canonical_tuple(*boundary, colocate_column_count)) {
                    break;
                }
                boundary = &candidate_ranges[j]->max;
                i = j;
            }

            if (tablet_range != nullptr && !tablet_range->strictly_contains(*boundary)) {
                // Cannot place boundary outside tablet range, keep accumulating.
                continue;
            }

            boundaries.push_back(*boundary);
            accumulated = 0;

            if (static_cast<int32_t>(boundaries.size()) >= actual_split_count - 1) {
                break;
            }
        }
    }

    if (boundaries.empty()) {
        return result;
    }

    // Step 5: Estimate per-range data sizes by walking candidate ranges against boundaries.
    int32_t num_splits = static_cast<int32_t>(boundaries.size()) + 1;
    result.boundaries = std::move(boundaries);
    result.range_data_sizes.resize(num_splits, 0);
    result.range_num_rows.resize(num_splits, 0);
    if (track_sources) {
        result.range_source_stats.resize(num_splits);
    }

    {
        size_t boundary_index = 0;
        for (const auto* range : candidate_ranges) {
            while (boundary_index < result.boundaries.size() &&
                   range->max.compare(result.boundaries[boundary_index]) > 0) {
                boundary_index++;
            }
            int32_t group_index = std::min(static_cast<int32_t>(boundary_index), num_splits - 1);
            result.range_data_sizes[group_index] += range->data_size;
            result.range_num_rows[group_index] += range->num_rows;

            if (track_sources) {
                for (const auto& [source_id, stats_pair] : range->source_stats) {
                    auto& dest = result.range_source_stats[group_index][source_id];
                    dest.first += stats_pair.first;
                    dest.second += stats_pair.second;
                }
            }
        }
    }

    return result;
}

// ================================================================================
// Tablet splitting (uses core algorithm above)
// ================================================================================

namespace {

// Per-rowset anchor totals taken from the parent's recorded metadata. Used to
// renormalize per-split-group estimates so Σ children equals parent exactly,
// preserving stat conservation across re-splits regardless of how the
// underlying segment-distribution model approximates straddling sub-segments.
struct RowsetAnchor {
    int64_t num_rows = 0;
    int64_t data_size = 0;
    int64_t num_dels = 0;
};

// Build the per-rowset anchor map from the parent tablet metadata. For PK
// tablets without a populated num_dels field on the rowset (legacy
// metadata), derive num_dels from the delvec — same fallback as the
// pre-anchor code path. If a rowset reports num_dels > num_rows
// (pathological metadata), clamp up front with a WARNING; the
// cap-and-redistribute contract assumes parent.num_dels <= parent.num_rows.
std::unordered_map<uint32_t, RowsetAnchor> build_rowset_anchor(const TabletMetadataPB& metadata,
                                                               TabletManager* tablet_manager) {
    std::unordered_map<uint32_t, RowsetAnchor> anchor;
    anchor.reserve(metadata.rowsets_size());
    const bool pk = is_primary_key(metadata);
    for (const auto& rowset : metadata.rowsets()) {
        RowsetAnchor a;
        // Anchor totals: prefer the rowset-level fields. When legacy /
        // incomplete metadata omits them, fall back to summing the
        // segment-level fields. Without this fallback, anchor=0 collapses
        // every child's stat to 0 even though segment metadata still
        // carries real values — the pre-anchor path implicitly used the
        // segment-derived numbers via range_source_stats, so we preserve
        // that property explicitly here.
        if (rowset.has_num_rows()) {
            a.num_rows = rowset.num_rows();
        } else {
            for (const auto& sm : rowset.segment_metas()) {
                a.num_rows += sm.num_rows();
            }
        }
        if (rowset.has_data_size()) {
            a.data_size = rowset.data_size();
        } else {
            for (int i = 0; i < rowset.segment_size_size(); ++i) {
                a.data_size += rowset.segment_size(i);
            }
        }
        if (rowset.has_num_dels()) {
            a.num_dels = rowset.num_dels();
        } else if (pk && tablet_manager != nullptr) {
            // Legacy fallback: derive num_dels from the delvec. Costs one
            // delvec read per rowset, acceptable on the one-shot split path.
            a.num_dels = static_cast<int64_t>(tablet_manager->update_mgr()->get_rowset_num_deletes(metadata, rowset));
        }
        if (a.num_dels > a.num_rows) {
            LOG(WARNING) << "rowset id=" << rowset.id() << " has num_dels=" << a.num_dels
                         << " > num_rows=" << a.num_rows << "; clamping for split allocation";
            a.num_dels = a.num_rows;
        }
        anchor.emplace(rowset.id(), a);
    }
    return anchor;
}

// Anchor each split group's per-rowset stats to the parent's recorded totals
// using the Hare-Niemeyer helper. Σ children stat == parent stat exactly for
// num_rows, data_size, and num_dels per rowset (modulo cap-and-redistribute
// for invalid parents — see tablet_reshard_helper.h contracts).
//
// Replaces the pre-anchor flow which wrote per-source weights raw into
// rowset_stats and ran a separate num_dels Hare-Niemeyer pass after.
void apply_rowset_anchor(const std::unordered_map<uint32_t, RowsetAnchor>& anchor, const RangeSplitResult& split_result,
                         std::vector<TabletRangeInfo>* split_ranges) {
    DCHECK(split_ranges != nullptr);
    const int64_t num_splits = static_cast<int64_t>(split_ranges->size());
    if (num_splits == 0) return;

    for (const auto& [source_id, ra] : anchor) {
        // Gather per-group weights for this source. Missing entries
        // contribute weight 0; zero-weight buckets receive zero allocation
        // (or share the uniform fallback when every weight is zero).
        std::vector<int64_t> rows_w(num_splits, 0);
        std::vector<int64_t> bytes_w(num_splits, 0);
        for (int64_t g = 0; g < num_splits; ++g) {
            if (g >= static_cast<int64_t>(split_result.range_source_stats.size())) break;
            auto it = split_result.range_source_stats[g].find(source_id);
            if (it != split_result.range_source_stats[g].end()) {
                rows_w[g] = it->second.first;
                bytes_w[g] = it->second.second;
            }
        }

        std::vector<int64_t> rows_alloc(num_splits, 0);
        std::vector<int64_t> bytes_alloc(num_splits, 0);
        std::vector<int64_t> dels_alloc(num_splits, 0);
        tablet_reshard_helper::allocate_proportionally(ra.num_rows, rows_w, &rows_alloc);
        tablet_reshard_helper::allocate_proportionally(ra.data_size, bytes_w, &bytes_alloc);
        // num_dels follows the row distribution: deletes are per-row, not
        // per-byte, so byte weights would skew the split when row and byte
        // distributions disagree (e.g. compressed columns).
        tablet_reshard_helper::allocate_proportionally(ra.num_dels, rows_w, &dels_alloc);
        tablet_reshard_helper::cap_and_redistribute_dels(rows_alloc, &dels_alloc);

        for (int64_t g = 0; g < num_splits; ++g) {
            // Skip writes that would create an empty entry — keeps the
            // rowset_stats map sparse, identical to the pre-anchor behavior
            // for sources with no representation in the group.
            if (rows_alloc[g] == 0 && bytes_alloc[g] == 0 && dels_alloc[g] == 0) {
                continue;
            }
            auto& dst = (*split_ranges)[g].rowset_stats[source_id];
            dst.num_rows = rows_alloc[g];
            dst.data_size = bytes_alloc[g];
            dst.num_dels = dels_alloc[g];
        }
    }
}

// Build SegmentSplitInfo[] from a tablet's rowsets. Rejects per-rowset segment
// metadata that is internally inconsistent (segments / segment_size /
// segment_metas array sizes diverge). Does NOT enforce "segments non-empty" —
// the data-driven and external-boundaries callers have different semantics on empty (one
// errors, the other treats it as a no-op fast path) and own that check.
Status build_segments_from_rowsets(const RepeatedPtrField<RowsetMetadataPB>& rowsets,
                                   std::vector<SegmentSplitInfo>* segments) {
    for (const auto& rowset : rowsets) {
        if (rowset.segments_size() != rowset.segment_size_size() ||
            rowset.segments_size() != rowset.segment_metas_size()) {
            return Status::InvalidArgument("Segment metadata is inconsistent with segment list");
        }
        for (int32_t i = 0; i < rowset.segments_size(); ++i) {
            SegmentSplitInfo segment;
            segment.source_id = rowset.id();
            const auto& segment_meta = rowset.segment_metas(i);
            RETURN_IF_ERROR(segment.min_key.from_proto(segment_meta.sort_key_min()));
            RETURN_IF_ERROR(segment.max_key.from_proto(segment_meta.sort_key_max()));
            segment.num_rows = segment_meta.num_rows();
            segment.data_size = rowset.segment_size(i);
            RETURN_IF_ERROR(segment.load_sort_key_samples(segment_meta));
            segments->push_back(std::move(segment));
        }
    }
    return Status::OK();
}

// Compute split_count tablet ranges covering the tablet's key space.
//
// Postcondition on Status::OK: split_ranges->size() == split_count.
// When the algorithm cannot produce exactly that many ranges (insufficient
// boundary points given the segment key distribution), returns
// Status::InvalidArgument with split_ranges cleared. The caller (split_tablet)
// is expected to fall back to identical-tablet publish; only new_tablet_ids(0)
// is consumed in that case, and FE is responsible for reclaiming the remaining
// preallocated tablet ids.
//
// colocate_column_count > 0 enables colocate-aware boundary canonicalization (see
// calculate_range_split_boundaries). A malformed FE that sends colocate_column_count
// larger than the sort-key arity returns Status::InvalidArgument here, which triggers
// the same identical-tablet fallback as "no boundaries" — preserves the publish loop
// instead of hard-failing.
Status get_tablet_split_ranges_impl(TabletManager* tablet_manager, const TabletMetadataPtr& tablet_metadata,
                                    int32_t split_count, std::vector<TabletRangeInfo>* split_ranges,
                                    int32_t colocate_column_count) {
    if (split_count < 2) {
        return Status::InvalidArgument("Invalid split count, it is less than 2");
    }
    if (colocate_column_count < 0 || colocate_column_count > tablet_metadata->schema().sort_key_idxes_size()) {
        return Status::InvalidArgument(fmt::format("Invalid colocate_column_count {}, sort key arity is {}",
                                                   colocate_column_count,
                                                   tablet_metadata->schema().sort_key_idxes_size()));
    }

    std::vector<SegmentSplitInfo> segments;
    RETURN_IF_ERROR(build_segments_from_rowsets(tablet_metadata->rowsets(), &segments));
    if (segments.empty()) {
        return Status::InvalidArgument("No segments found in tablet metadata");
    }

    // Step 2: Calculate split boundaries with tablet range filtering.
    TabletRange tablet_range;
    RETURN_IF_ERROR(tablet_range.from_proto(tablet_metadata->range()));

    int64_t total_num_rows = 0;
    for (const auto& segment : segments) {
        total_num_rows += segment.num_rows;
    }
    int64_t avg_num_rows = std::max<int64_t>(1, total_num_rows / split_count);

    ASSIGN_OR_RETURN(auto split_result,
                     calculate_range_split_boundaries(segments, split_count, avg_num_rows,
                                                      /*use_num_rows=*/true,
                                                      /*track_sources=*/true, &tablet_range, colocate_column_count));

    if (split_result.boundaries.empty()) {
        return Status::InvalidArgument("Not enough split ranges available");
    }

    // Step 3: Build TabletRangeInfo directly from result.
    int32_t num_splits = static_cast<int32_t>(split_result.boundaries.size()) + 1;

    DCHECK(split_ranges->empty());
    split_ranges->reserve(num_splits);

    for (int32_t i = 0; i < num_splits; i++) {
        auto& sr = split_ranges->emplace_back();
        if (i == 0) {
            sr.range = tablet_metadata->range();
        } else {
            split_result.boundaries[i - 1].to_proto(sr.range.mutable_lower_bound());
            sr.range.set_lower_bound_included(true);
        }

        if (i < num_splits - 1) {
            split_result.boundaries[i].to_proto(sr.range.mutable_upper_bound());
            sr.range.set_upper_bound_included(false);
        } else {
            if (tablet_metadata->range().has_upper_bound()) {
                sr.range.mutable_upper_bound()->CopyFrom(tablet_metadata->range().upper_bound());
                sr.range.set_upper_bound_included(tablet_metadata->range().upper_bound_included());
            } else {
                sr.range.clear_upper_bound();
                sr.range.clear_upper_bound_included();
            }
        }
    }

    if (split_ranges->size() != static_cast<size_t>(split_count)) {
        LOG(WARNING) << "Insufficient split boundaries: tablet_id=" << tablet_metadata->id()
                     << " version=" << tablet_metadata->version() << " requested=" << split_count
                     << " produced=" << split_ranges->size();
        auto produced = split_ranges->size();
        split_ranges->clear();
        return Status::InvalidArgument(
                fmt::format("Insufficient split boundaries: requested {}, produced {}", split_count, produced));
    }

    // Anchor per-split per-rowset stats to the parent's recorded totals so
    // that Σ children stat == parent stat exactly for num_rows / data_size /
    // num_dels. The segment-level distribution from
    // calculate_range_split_boundaries is used as relative weights only;
    // absolute values come from the parent metadata. This eliminates the
    // residual drift seen across multi-level splits when the algorithm
    // re-runs on the same physical segments under a different
    // ordered_boundaries set.
    auto anchor = build_rowset_anchor(*tablet_metadata, tablet_manager);
    apply_rowset_anchor(anchor, split_result, split_ranges);

    return Status::OK();
}

// Builds a single new-tablet metadata that is "identical" to the old tablet —
// inherits rowsets / delvec_meta / sstable_meta / dcg_meta / rowset_to_schema as-is,
// only rebinds id / version / commit_time / gtid and clears the per-version
// transient fields (compaction_inputs, orphan_files, prev_garbage_version).
//
// Used by split_tablet's symmetric identical-fallback path. Both the
// data-driven branch (when get_tablet_split_ranges fails) and the external boundaries
// branch (when compute_split_ranges_from_external_boundaries fails) call
// this helper with new_tablet_ids[0] to materialize a single identical
// new tablet rather than the requested K new tablets.
//
// In both cases the old tablet may be non-empty; the identical new tablet
// must inherit its data so the load can write into it without data loss.
MutableTabletMetadataPtr make_identical_new_tablet_metadata(const TabletMetadataPtr& old_tablet_metadata,
                                                            int64_t new_id, int64_t new_version,
                                                            const TxnInfoPB& txn_info) {
    auto new_tablet_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_metadata);
    new_tablet_metadata->set_id(new_id);
    new_tablet_metadata->set_version(new_version);
    new_tablet_metadata->set_commit_time(txn_info.commit_time());
    new_tablet_metadata->set_gtid(txn_info.gtid());
    new_tablet_metadata->clear_compaction_inputs();
    new_tablet_metadata->clear_orphan_files();
    new_tablet_metadata->clear_prev_garbage_version();
    return new_tablet_metadata;
}

// external-boundaries peer of get_tablet_split_ranges: produces a vector<TabletRangeInfo>
// from FE-supplied boundaries instead of computing them from segment
// distribution. Reuses distribute_segment_to_ranges, build_rowset_anchor, and
// apply_rowset_anchor so the per-rowset stat math is identical to the
// data-driven path. Inline numbered step tags document each phase.
//
// Non-OK return is caller-handled (split_tablet) by routing to the symmetric
// identical-fallback path with the external-boundaries bvar bumped. The helper does NOT
// fall back internally; it surfaces the precise reason via Status::message.
Status compute_split_ranges_from_external_boundaries_impl(TabletManager* tablet_manager,
                                                          const TabletMetadataPtr& old_tablet_metadata,
                                                          const RepeatedPtrField<TabletRangePB>& external_ranges,
                                                          std::vector<TabletRangeInfo>* split_ranges) {
    DCHECK(split_ranges != nullptr);
    DCHECK(split_ranges->empty());

    // 1a. Structural validation (schema-free): closed-open per range, no
    //     byte-equal zero-width child, first.lower matches parent.lower,
    //     last.upper matches parent.upper, adjacent ranges meet exactly.
    RETURN_IF_ERROR(TabletRangeHelper::validate_new_tablet_ranges(old_tablet_metadata->range(), external_ranges));

    // 1b. Schema-aware validation of FE-supplied TuplePB bounds. Without
    //     this, DatumVariant::compare's DCHECK-only type guard means
    //     release builds silently mis-compare malformed FE input.
    //     For decimal-family columns, also validate precision/scale exactly:
    //     DecimalTypeInfo compares raw unscaled values, so a bound with the
    //     same LogicalType but different scale than the schema would
    //     mis-order against schema-derived segment bounds.
    //     Reads schema metadata directly from TabletSchemaPB to avoid the
    //     heavy TabletSchema::create allocation; only sort-key columns'
    //     type / precision / scale are needed.
    const TabletSchemaPB& schema_pb = old_tablet_metadata->schema();
    const auto& sort_key_idxes = schema_pb.sort_key_idxes();
    if (sort_key_idxes.empty()) {
        return Status::InvalidArgument("tablet has no sort key columns; external-boundaries path requires a sort key");
    }
    // Precompute sort-key column metadata once (constant per call); the per-tuple
    // validation runs 2K times (K boundaries × {lower, upper}) and each call would
    // otherwise rerun string_to_logical_type and re-read col_pb fields.
    struct SortKeyColumnMeta {
        LogicalType type;
        int precision;
        int scale;
        bool is_decimal;
    };
    std::vector<SortKeyColumnMeta> sort_key_meta;
    sort_key_meta.reserve(sort_key_idxes.size());
    for (int column_index : sort_key_idxes) {
        // ColumnPB.frac is the scale field (legacy naming).
        const auto& column_pb = schema_pb.column(column_index);
        const LogicalType type = string_to_logical_type(column_pb.type());
        const bool is_decimal = (type == TYPE_DECIMAL || type == TYPE_DECIMALV2 || is_decimalv3_field_type(type));
        sort_key_meta.push_back({type, column_pb.precision(), column_pb.frac(), is_decimal});
    }
    auto validate_tuple_against_schema = [&](const TuplePB& tuple, int range_index, std::string_view side) -> Status {
        if (tuple.values_size() != sort_key_idxes.size()) {
            return Status::InvalidArgument(fmt::format("new_tablet_ranges[{}].{}: tuple arity {} != sort_key arity {}",
                                                       range_index, side, tuple.values_size(), sort_key_idxes.size()));
        }
        for (int j = 0; j < tuple.values_size(); ++j) {
            const auto& variant = tuple.values(j);
            // external sort-key bounds are scalar values only — tablet sort
            // keys are restricted to scalar types (no ARRAY/MAP/STRUCT).
            // Enforce a single SCALAR PTypeNode with scalar_type set so
            // malformed FE input cannot reach the underlying
            // TypeDescriptor PTypeNode constructor, which DCHECKs and
            // does unguarded types.Get() on complex-type child indices
            // (be/src/types/type_descriptor.cpp:158-204).
            if (!variant.has_type() || variant.type().types_size() != 1) {
                return Status::InvalidArgument(
                        fmt::format("new_tablet_ranges[{}].{}: column[{}] variant must have exactly one type node "
                                    "(got types_size={})",
                                    range_index, side, j, variant.type().types_size()));
            }
            const auto& node = variant.type().types(0);
            if (static_cast<TTypeNodeType::type>(node.type()) != TTypeNodeType::SCALAR || !node.has_scalar_type()) {
                return Status::InvalidArgument(
                        fmt::format("new_tablet_ranges[{}].{}: column[{}] variant must be a scalar type "
                                    "(got node_type={}, has_scalar_type={})",
                                    range_index, side, j, node.type(), node.has_scalar_type()));
            }
            // Read variant type directly from the validated scalar_type instead of
            // constructing a full TypeDescriptor; saves the per-tuple allocation.
            const auto& scalar_type = node.scalar_type();
            const LogicalType variant_type = thrift_to_type(static_cast<TPrimitiveType::type>(scalar_type.type()));
            const auto& meta = sort_key_meta[j];
            if (variant_type != meta.type) {
                return Status::InvalidArgument(fmt::format(
                        "new_tablet_ranges[{}].{}: column[{}] variant type {} != schema type {}", range_index, side, j,
                        logical_type_to_string(variant_type), logical_type_to_string(meta.type)));
            }
            if (meta.is_decimal) {
                const int variant_precision = scalar_type.has_precision() ? scalar_type.precision() : -1;
                const int variant_scale = scalar_type.has_scale() ? scalar_type.scale() : -1;
                if (variant_precision != meta.precision || variant_scale != meta.scale) {
                    return Status::InvalidArgument(fmt::format(
                            "new_tablet_ranges[{}].{}: column[{}] decimal precision/scale "
                            "({}, {}) != schema ({}, {})",
                            range_index, side, j, variant_precision, variant_scale, meta.precision, meta.scale));
                }
            }
        }
        return Status::OK();
    };
    for (int i = 0; i < external_ranges.size(); ++i) {
        const auto& external_range = external_ranges[i];
        if (external_range.has_lower_bound()) {
            RETURN_IF_ERROR(validate_tuple_against_schema(external_range.lower_bound(), i, "lower"));
        }
        if (external_range.has_upper_bound()) {
            RETURN_IF_ERROR(validate_tuple_against_schema(external_range.upper_bound(), i, "upper"));
        }
    }

    // 2. Seed K TabletRangeInfo from external_ranges; rowset_stats stays empty
    //    for now (filled by apply_rowset_anchor in step 10).
    split_ranges->reserve(external_ranges.size());
    for (const auto& external_range : external_ranges) {
        auto& range_info = split_ranges->emplace_back();
        range_info.range = external_range;
    }

    // 3. Parse FE bounds into VariantTuple + track explicit-presence. An
    //    unset side (only possible at first.lo / last.hi per structural
    //    validation) means ±infinity and is excluded from the per-range
    //    inversion check at step 3a. Adjacent (3b) bounds are always
    //    explicit (interior boundaries are required to be set).
    struct ParsedBounds {
        VariantTuple lo;
        VariantTuple hi;
        bool lo_explicit = false;
        bool hi_explicit = false;
    };
    // validate_new_tablet_ranges already enforces that first.has_lower_bound() ==
    // parent.has_lower_bound() and last.has_upper_bound() == parent.has_upper_bound(),
    // and that interior bounds are always set. So an absent external bound here means
    // parent is unbounded on that side too — no parent inheritance is ever needed.
    std::vector<ParsedBounds> parsed(external_ranges.size());
    for (int i = 0; i < external_ranges.size(); ++i) {
        const auto& external_range = external_ranges[i];
        if (external_range.has_lower_bound()) {
            RETURN_IF_ERROR(parsed[i].lo.from_proto(external_range.lower_bound()));
            parsed[i].lo_explicit = true;
        }
        if (external_range.has_upper_bound()) {
            RETURN_IF_ERROR(parsed[i].hi.from_proto(external_range.upper_bound()));
            parsed[i].hi_explicit = true;
        }
    }

    // 3a. Per-range strict semantic check. '>=' catches inversion AND
    //     semantic zero-width (e.g., byte-distinct but same value under
    //     schema-aware comparison). Skip when either side is ±infinity.
    for (int i = 0; i < external_ranges.size(); ++i) {
        if (!parsed[i].lo_explicit || !parsed[i].hi_explicit) continue;
        if (parsed[i].lo.compare(parsed[i].hi) >= 0) {
            return Status::InvalidArgument(
                    fmt::format("new_tablet_ranges[{}] semantically inverted/zero-width (lower >= upper)", i));
        }
    }

    // 3b. Adjacent monotonic check. Interior bounds are always explicit.
    for (int i = 0; i + 1 < external_ranges.size(); ++i) {
        if (parsed[i].hi.compare(parsed[i + 1].lo) > 0) {
            return Status::InvalidArgument(fmt::format("new_tablet_ranges[{}/{}] semantically out of order", i, i + 1));
        }
    }

    // 4. Empty old tablet fast-path: no rowsets to distribute, K
    //    TabletRangeInfo with empty rowset_stats is the correct output.
    //    build_new_tablets_from_split_ranges will produce K new tablets with
    //    no rowsets. Runs AFTER FE-input validation so empty tablets via external boundaries
    //    cannot accept semantically-invalid ranges either.
    if (old_tablet_metadata->rowsets_size() == 0) {
        return Status::OK();
    }

    // 5. Build SegmentSplitInfo from rowsets via shared helper. Empty-segments
    //    here is corruption (we already short-circuited the empty-rowsets case
    //    at step 4); external boundaries uses a distinct error message vs the data-driven path.
    std::vector<SegmentSplitInfo> segments;
    RETURN_IF_ERROR(build_segments_from_rowsets(old_tablet_metadata->rowsets(), &segments));
    if (segments.empty()) {
        return Status::InvalidArgument("rowsets present but no segments derived (possibly corrupt metadata)");
    }

    // 6. Compute segment envelope and effective envelope.
    VariantTuple seg_min = segments.front().min_key;
    VariantTuple seg_max = segments.front().max_key;
    for (const auto& s : segments) {
        if (s.min_key.compare(seg_min) < 0) seg_min = s.min_key;
        if (s.max_key.compare(seg_max) > 0) seg_max = s.max_key;
    }
    VariantTuple effective_lo = seg_min;
    VariantTuple effective_hi = seg_max;
    if (old_tablet_metadata->range().has_lower_bound()) {
        VariantTuple parent_lo;
        RETURN_IF_ERROR(parent_lo.from_proto(old_tablet_metadata->range().lower_bound()));
        if (parent_lo.compare(effective_lo) > 0) effective_lo = parent_lo;
    }
    if (old_tablet_metadata->range().has_upper_bound()) {
        VariantTuple parent_hi;
        RETURN_IF_ERROR(parent_hi.from_proto(old_tablet_metadata->range().upper_bound()));
        if (parent_hi.compare(effective_hi) < 0) effective_hi = parent_hi;
    }
    // '>=' covers both empty (lo > hi) and degenerate-point (lo == hi)
    // envelopes. Half-open range math cannot K-way split a point; for P0
    // both cases return InvalidArgument and the caller falls back.
    if (effective_lo.compare(effective_hi) >= 0) {
        return Status::InvalidArgument(
                "effective envelope empty or degenerate-point (segments don't overlap parent's range or collapse to a "
                "single key)");
    }

    // 7. Build distribution vector covering the FULL segment envelope.
    //    dist_to_final[j] == -1 for sinks, otherwise the index into split_ranges.
    //    Non-explicit ±infinity sides clip to the effective envelope edge.
    std::vector<RangeInfo> dist_ranges;
    std::vector<int> dist_to_final;
    dist_ranges.reserve(external_ranges.size() + 2);
    dist_to_final.reserve(external_ranges.size() + 2);
    if (seg_min.compare(effective_lo) < 0) {
        RangeInfo sink;
        sink.min = seg_min;
        sink.max = effective_lo;
        dist_ranges.push_back(std::move(sink));
        dist_to_final.push_back(-1);
    }
    for (int i = 0; i < external_ranges.size(); ++i) {
        const VariantTuple clipped_lo =
                parsed[i].lo_explicit ? ((parsed[i].lo.compare(effective_lo) < 0) ? effective_lo : parsed[i].lo)
                                      : effective_lo;
        const VariantTuple clipped_hi =
                parsed[i].hi_explicit ? ((parsed[i].hi.compare(effective_hi) > 0) ? effective_hi : parsed[i].hi)
                                      : effective_hi;
        // Skip on '>=' (zero-width too): [x,x) cannot contain any row under
        // closed-open semantics; including would risk closed-last-range edge.
        if (clipped_lo.compare(clipped_hi) >= 0) continue;
        RangeInfo ri;
        ri.min = clipped_lo;
        ri.max = clipped_hi;
        dist_ranges.push_back(std::move(ri));
        dist_to_final.push_back(i);
    }
    if (seg_max.compare(effective_hi) > 0) {
        RangeInfo sink;
        sink.min = effective_hi;
        sink.max = seg_max;
        dist_ranges.push_back(std::move(sink));
        dist_to_final.push_back(-1);
    }
    // Defense-in-depth: should be unreachable under v20 invariants (envelope
    // check at step 5 ensures effective_lo < effective_hi; FE ranges tile
    // parent's range; at least one FE range must overlap effective envelope).
    // Asserted at runtime so a future regression in invariants doesn't OOB
    // find_overlapping_ranges' last_range_index = size()-1.
    if (dist_ranges.empty()) {
        return Status::InvalidArgument("distribution vector empty (degenerate envelope or all active slots skipped)");
    }

    // 8. Distribute segments into the full distribution vector. Out-of-parent
    //    data flows into sink buckets; sink stats are discarded at step 10.
    for (const auto& seg : segments) {
        distribute_segment_to_ranges(seg, dist_ranges, /*track_sources=*/true);
    }

    // 9. Build anchor BEFORE the sanity check. anchor's per-rowset totals
    //    fall back to summing segment_metas when rowset-level num_rows /
    //    data_size are absent on legacy metadata, so the sanity check below
    //    using anchor totals catches legacy rowsets that direct
    //    rowset.num_rows() access would miss.
    auto anchor = build_rowset_anchor(*old_tablet_metadata, tablet_manager);

    // 10. Per-axis all-zero sanity check. For each rowset where anchor totals
    //     are positive, require Σ over active slots > 0 on the matching axis.
    //     allocate_proportionally falls back to uniform allocation when all
    //     weights are zero, which would silently invent stats.
    for (const auto& [rowset_id, ra] : anchor) {
        const bool need_row_weight = (ra.num_rows > 0);
        const bool need_byte_weight = (ra.data_size > 0);
        if (!need_row_weight && !need_byte_weight) continue;
        int64_t active_rows_w = 0;
        int64_t active_bytes_w = 0;
        for (size_t j = 0; j < dist_ranges.size(); ++j) {
            if (dist_to_final[j] < 0) continue; // skip sinks
            auto it = dist_ranges[j].source_stats.find(rowset_id);
            if (it != dist_ranges[j].source_stats.end()) {
                active_rows_w += it->second.first;
                active_bytes_w += it->second.second;
            }
        }
        if (need_row_weight && active_rows_w == 0) {
            return Status::InvalidArgument(fmt::format(
                    "rowset id={} has anchor.num_rows={} but no active row weight (possibly corrupt metadata)",
                    rowset_id, ra.num_rows));
        }
        if (need_byte_weight && active_bytes_w == 0) {
            return Status::InvalidArgument(fmt::format(
                    "rowset id={} has anchor.data_size={} but no active byte weight (possibly corrupt metadata)",
                    rowset_id, ra.data_size));
        }
    }

    // 11. Synthesize RangeSplitResult with K slots; active slots get their
    //     accumulated source_stats, sinks discarded, skipped slots stay empty.
    RangeSplitResult fake_result;
    fake_result.range_source_stats.resize(external_ranges.size());
    for (size_t j = 0; j < dist_ranges.size(); ++j) {
        if (dist_to_final[j] < 0) continue;
        fake_result.range_source_stats[dist_to_final[j]] = dist_ranges[j].source_stats;
    }

    // 12. Anchor: Σ children stat == parent stat exactly for num_rows /
    //     data_size / num_dels. Writes split_ranges[i].rowset_stats.
    apply_rowset_anchor(anchor, fake_result, split_ranges);

    return Status::OK();
}

// Build K new-tablet metadata entries from a pre-computed split_ranges vector.
// Source-agnostic: the caller decides how split_ranges was produced (data-driven
// boundary search, FE-supplied external boundaries, or any future variant). Each
// new tablet inherits the old tablet's schema/config, gets a fresh
// id/version/commit_time/gtid/range, has per-version transient fields cleared,
// and its rowset list is restricted to the new range with per-rowset stats
// applied from split_ranges[i].rowset_stats.
//
// Precondition: split_ranges.size() == splitting_tablet.new_tablet_ids_size().
// Per-rowset stats are honored when present in split_ranges[i].rowset_stats;
// otherwise the rowset is preserved in the new tablet's metadata (still
// referencing the same shared segment files) but with num_rows / data_size /
// num_dels set to 0, signaling "rowset is visible in this child's metadata
// but contributes no rows here under the child's range".
StatusOr<std::unordered_map<int64_t, MutableTabletMetadataPtr>> build_new_tablets_from_split_ranges(
        const TabletMetadataPtr& old_tablet_metadata, const SplittingTabletInfoPB& splitting_tablet,
        int64_t new_version, const TxnInfoPB& txn_info, const std::vector<TabletRangeInfo>& split_ranges) {
    // Defense-in-depth: callers (get_tablet_split_ranges and external-boundaries helper) are
    // contracted to return split_ranges.size() == new_tablet_ids_size() on OK,
    // but a runtime check here prevents OOB reads if the contract is ever
    // broken by a future refactor (Release builds strip DCHECK).
    if (split_ranges.size() != static_cast<size_t>(splitting_tablet.new_tablet_ids_size())) {
        return Status::InternalError(fmt::format("split_ranges size mismatch: expected={}, actual={}",
                                                 splitting_tablet.new_tablet_ids_size(), split_ranges.size()));
    }

    std::unordered_map<int64_t, MutableTabletMetadataPtr> new_metadatas;
    new_metadatas.reserve(splitting_tablet.new_tablet_ids_size());

    for (int32_t i = 0; i < splitting_tablet.new_tablet_ids_size(); ++i) {
        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_metadata);
        new_tablet_new_metadata->set_id(splitting_tablet.new_tablet_ids(i));
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        new_tablet_new_metadata->clear_compaction_inputs();
        new_tablet_new_metadata->clear_orphan_files();
        new_tablet_new_metadata->clear_prev_garbage_version();
        new_tablet_new_metadata->mutable_range()->CopyFrom(split_ranges[i].range);
        tablet_reshard_helper::set_all_data_files_shared(new_tablet_new_metadata.get());

        for (auto& rowset_metadata : *new_tablet_new_metadata->mutable_rowsets()) {
            RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_range(&rowset_metadata, split_ranges[i].range));
            const auto it = split_ranges[i].rowset_stats.find(rowset_metadata.id());
            if (it != split_ranges[i].rowset_stats.end()) {
                // apply_rowset_anchor + cap_and_redistribute_dels guarantee
                // num_dels <= num_rows for every (rowset, child). The std::min
                // below is defense-in-depth against an upstream regression.
                DCHECK_LE(it->second.num_dels, it->second.num_rows);
                int64_t scaled_num_dels = std::min<int64_t>(it->second.num_dels, it->second.num_rows);
                rowset_metadata.set_num_rows(it->second.num_rows);
                rowset_metadata.set_data_size(it->second.data_size);
                rowset_metadata.set_num_dels(scaled_num_dels);
            } else {
                rowset_metadata.set_num_rows(0);
                rowset_metadata.set_data_size(0);
                rowset_metadata.set_num_dels(0);
            }
        }

        new_metadatas.emplace(new_tablet_new_metadata->id(), std::move(new_tablet_new_metadata));
    }

    return new_metadatas;
}

} // namespace

// Public wrapper for the anon-namespace implementation. Exposed via
// tablet_splitter.h so unit tests can drive the validation paths directly
// without spinning up a TabletManager/filesystem fixture.
Status compute_split_ranges_from_external_boundaries(TabletManager* tablet_manager,
                                                     const TabletMetadataPtr& old_tablet_metadata,
                                                     const RepeatedPtrField<TabletRangePB>& external_ranges,
                                                     std::vector<TabletRangeInfo>* split_ranges) {
    return compute_split_ranges_from_external_boundaries_impl(tablet_manager, old_tablet_metadata, external_ranges,
                                                              split_ranges);
}

// Public wrapper for the data-driven split-ranges helper. Exposed for parity
// testing against compute_split_ranges_from_external_boundaries; production
// call site is split_tablet().
Status get_tablet_split_ranges(TabletManager* tablet_manager, const TabletMetadataPtr& tablet_metadata,
                               int32_t split_count, std::vector<TabletRangeInfo>* split_ranges,
                               int32_t colocate_column_count) {
    return get_tablet_split_ranges_impl(tablet_manager, tablet_metadata, split_count, split_ranges,
                                        colocate_column_count);
}

StatusOr<std::unordered_map<int64_t, MutableTabletMetadataPtr>> split_tablet(
        TabletManager* tablet_manager, const TabletMetadataPtr& tablet_metadata,
        const SplittingTabletInfoPB& splitting_tablet, int64_t new_version, const TxnInfoPB& txn_info) {
    if (tablet_metadata == nullptr) {
        return Status::InvalidArgument("tablet metadata is null");
    }
    if (splitting_tablet.new_tablet_ids_size() <= 0) {
        return Status::InvalidArgument("splitting tablet has no new tablet");
    }

    // Flush the parent's PK-index memtable into sstables before propagating
    // metadata to the children, so every child inherits an sstable_meta that
    // already covers its rowsets' live data. This is the pre-split half of
    // the "reshard inputs must have full sstable coverage" invariant; merge
    // does the post-split half in merge_sstables.
    ASSIGN_OR_RETURN(TabletMetadataPtr old_tablet_metadata,
                     tablet_manager->update_mgr()->flush_pk_memtable(tablet_metadata));

    // Dispatch on FE-supplied new_tablet_ranges. When set, FE has computed the
    // K-1 boundaries externally (external boundaries / external boundaries); BE computes
    // per-rowset stats only. When unset, fall back to the existing
    // data-driven boundary search via get_tablet_split_ranges.
    //
    // Symmetric identical-fallback: either path's non-OK Status routes through
    // make_identical_new_tablet_metadata to produce a single identical new
    // tablet that inherits parent's data. Distinct bvar counters distinguish
    // the failure root cause for ops alerting.
    std::vector<TabletRangeInfo> split_ranges;
    // colocate_column_count is carried at the txn level (single split job = single txn) since
    // every SplittingTabletInfoPB in the same job would carry the same value. See lake_types.proto.
    // external-boundaries path skips boundary computation entirely (FE-supplied), so it does not consume the
    // colocate_column_count signal.
    const bool is_external_boundaries = splitting_tablet.new_tablet_ranges_size() > 0;
    // For external boundaries, FE supplies two parallel lists (new_tablet_ids and new_tablet_ranges);
    // they must agree in size, since downstream code zips them per index.
    Status status;
    if (is_external_boundaries && splitting_tablet.new_tablet_ranges_size() != splitting_tablet.new_tablet_ids_size()) {
        status = Status::InvalidArgument(fmt::format("new_tablet_ranges.size={} != new_tablet_ids.size={}",
                                                     splitting_tablet.new_tablet_ranges_size(),
                                                     splitting_tablet.new_tablet_ids_size()));
    } else if (is_external_boundaries) {
        status = compute_split_ranges_from_external_boundaries(tablet_manager, old_tablet_metadata,
                                                               splitting_tablet.new_tablet_ranges(), &split_ranges);
    } else {
        status = get_tablet_split_ranges(tablet_manager, old_tablet_metadata, splitting_tablet.new_tablet_ids_size(),
                                         &split_ranges, txn_info.colocate_column_count());
    }
    if (!status.ok()) {
        if (is_external_boundaries) {
            g_tablet_reshard_split_external_boundaries_fallback_total << 1;
            LOG(WARNING) << "external-boundaries validation/compute failed; identical fallback. "
                         << "tablet_id=" << old_tablet_metadata->id() << ", version=" << old_tablet_metadata->version()
                         << ", txn_id=" << txn_info.txn_id() << ", status=" << status;
        } else {
            g_tablet_reshard_split_fallback_total << 1;
            LOG(WARNING) << "Failed to get tablet split ranges, will not split this tablet: "
                         << old_tablet_metadata->id() << ", version: " << old_tablet_metadata->version()
                         << ", txn_id: " << txn_info.txn_id() << ", status: " << status;
        }
        auto new_tablet_metadata = make_identical_new_tablet_metadata(
                old_tablet_metadata, splitting_tablet.new_tablet_ids(0), new_version, txn_info);
        std::unordered_map<int64_t, MutableTabletMetadataPtr> new_metadatas;
        new_metadatas.emplace(new_tablet_metadata->id(), std::move(new_tablet_metadata));
        return new_metadatas;
    }

    return build_new_tablets_from_split_ranges(old_tablet_metadata, splitting_tablet, new_version, txn_info,
                                               split_ranges);
}

} // namespace starrocks::lake
