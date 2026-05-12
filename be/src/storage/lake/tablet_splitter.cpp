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
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/update_manager.h"
#include "storage/tablet_range.h"

extern bvar::Adder<int64_t> g_tablet_reshard_split_fallback_total;

namespace starrocks::lake {

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

struct Statistic {
    int64_t num_rows = 0;
    int64_t data_size = 0;
    int64_t num_dels = 0;
};

struct TabletRangeInfo {
    TabletRangePB range;
    std::unordered_map<uint32_t, Statistic> rowset_stats;
};

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
Status get_tablet_split_ranges(TabletManager* tablet_manager, const TabletMetadataPtr& tablet_metadata,
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

    for (const auto& rowset : tablet_metadata->rowsets()) {
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
            segments.push_back(std::move(segment));
        }
    }

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

} // namespace

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

    std::unordered_map<int64_t, MutableTabletMetadataPtr> new_metadatas;

    std::vector<TabletRangeInfo> split_ranges;
    // colocate_column_count is carried at the txn level (single split job = single txn) since
    // every SplittingTabletInfoPB in the same job would carry the same value. See lake_types.proto.
    Status status = get_tablet_split_ranges(tablet_manager, old_tablet_metadata, splitting_tablet.new_tablet_ids_size(),
                                            &split_ranges, txn_info.colocate_column_count());
    if (!status.ok()) {
        g_tablet_reshard_split_fallback_total << 1;
        LOG(WARNING) << "Failed to get tablet split ranges, will not split this tablet: " << old_tablet_metadata->id()
                     << ", version: " << old_tablet_metadata->version() << ", txn_id: " << txn_info.txn_id()
                     << ", status: " << status;
        auto new_tablet_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_metadata);
        new_tablet_metadata->set_id(splitting_tablet.new_tablet_ids(0));
        new_tablet_metadata->set_version(new_version);
        new_tablet_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_metadata->set_gtid(txn_info.gtid());
        new_tablet_metadata->clear_compaction_inputs();
        new_tablet_metadata->clear_orphan_files();
        new_tablet_metadata->clear_prev_garbage_version();
        new_metadatas.emplace(new_tablet_metadata->id(), std::move(new_tablet_metadata));
        return new_metadatas;
    }

    // Defense-in-depth: get_tablet_split_ranges guarantees
    // split_ranges.size() == new_tablet_ids_size() on OK, but a runtime check
    // here prevents OOB reads into split_ranges[i] if the contract is ever
    // broken by a future refactor (Release builds strip DCHECK).
    if (split_ranges.size() != static_cast<size_t>(splitting_tablet.new_tablet_ids_size())) {
        return Status::InternalError(fmt::format("split_ranges size mismatch: expected={}, actual={}",
                                                 splitting_tablet.new_tablet_ids_size(), split_ranges.size()));
    }
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

} // namespace starrocks::lake
