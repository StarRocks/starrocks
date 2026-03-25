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

#include <algorithm>
#include <set>
#include <unordered_map>
#include <vector>

#include "common/logging.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/tablet_range.h"

namespace starrocks::lake {

// ================================================================================
// Core range split algorithm (public API)
// ================================================================================

StatusOr<RangeSplitResult> calculate_range_split_boundaries(const std::vector<SegmentSplitInfo>& segments,
                                                            int32_t target_split_count, int64_t target_value_per_split,
                                                            bool use_num_rows, bool track_sources,
                                                            const TabletRange* tablet_range) {
    RangeSplitResult result;

    if (segments.empty() || target_split_count <= 1) {
        return result;
    }

    // Step 1: Collect all unique boundary points and sort them.
    auto comparator = [](const VariantTuple* a, const VariantTuple* b) { return a->compare(*b) < 0; };
    std::set<const VariantTuple*, decltype(comparator)> ordered_boundaries(comparator);
    for (const auto& seg : segments) {
        ordered_boundaries.insert(&seg.min_key);
        ordered_boundaries.insert(&seg.max_key);
    }

    if (ordered_boundaries.size() < 2) {
        return result;
    }

    // Step 2: Build ordered ranges between adjacent boundary points.
    struct RangeInfo {
        VariantTuple min;
        VariantTuple max;
        int64_t num_rows = 0;
        int64_t data_size = 0;
        SourceStats source_stats;
    };

    std::vector<RangeInfo> ordered_ranges;
    ordered_ranges.reserve(ordered_boundaries.size());
    const VariantTuple* last_boundary = nullptr;
    for (const auto* boundary : ordered_boundaries) {
        if (last_boundary != nullptr) {
            auto& range_info = ordered_ranges.emplace_back();
            range_info.min = *last_boundary;
            range_info.max = *boundary;
        }
        last_boundary = boundary;
    }

    if (ordered_ranges.empty()) {
        return result;
    }

    // Step 3: Distribute segment data proportionally across overlapping ranges.
    // Use binary search to find the first overlapping range for each segment.
    size_t last_range_idx = ordered_ranges.size() - 1;
    for (const auto& seg : segments) {
        // Binary search for the first range whose max_key could overlap with seg.min_key.
        size_t lo = 0, hi = ordered_ranges.size();
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            bool mid_is_last = (mid == last_range_idx);
            int cmp = ordered_ranges[mid].max.compare(seg.min_key);
            if (mid_is_last ? (cmp < 0) : (cmp <= 0)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // Scan forward from 'lo' to collect all overlapping ranges.
        std::vector<RangeInfo*> overlapping;
        for (size_t ri = lo; ri < ordered_ranges.size(); ri++) {
            auto& range = ordered_ranges[ri];
            if (range.min.compare(seg.max_key) > 0) {
                break;
            }
            if (ri != last_range_idx && range.min.compare(seg.max_key) >= 0) {
                break;
            }
            overlapping.push_back(&range);
        }

        if (overlapping.empty()) {
            continue;
        }

        auto count = static_cast<int64_t>(overlapping.size());

        int64_t rows_per_range = seg.num_rows / count;
        int64_t rows_remainder = seg.num_rows % count;
        int64_t size_per_range = seg.data_size / count;
        int64_t size_remainder = seg.data_size % count;

        for (int64_t i = 0; i < count; i++) {
            int64_t delta_rows = rows_per_range + (i < rows_remainder ? 1 : 0);
            int64_t delta_size = size_per_range + (i < size_remainder ? 1 : 0);

            overlapping[i]->num_rows += delta_rows;
            overlapping[i]->data_size += delta_size;

            if (track_sources) {
                auto& stats = overlapping[i]->source_stats[seg.source_id];
                stats.first += delta_rows;
                stats.second += delta_size;
            }
        }
    }

    // Step 4: Calculate split boundaries using a greedy algorithm.
    // If tablet_range is provided, only consider ranges that overlap with it.
    std::vector<const RangeInfo*> candidate_ranges;
    candidate_ranges.reserve(ordered_ranges.size());
    for (const auto& r : ordered_ranges) {
        if (tablet_range != nullptr && (tablet_range->less_than(r.min) || tablet_range->greater_than(r.max))) {
            continue;
        }
        candidate_ranges.push_back(&r);
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
        size_t boundary_idx = 0;
        for (const auto* range : candidate_ranges) {
            while (boundary_idx < result.boundaries.size() && range->max.compare(result.boundaries[boundary_idx]) > 0) {
                boundary_idx++;
            }
            int32_t group_idx = std::min(static_cast<int32_t>(boundary_idx), num_splits - 1);
            result.range_data_sizes[group_idx] += range->data_size;
            result.range_num_rows[group_idx] += range->num_rows;

            if (track_sources) {
                for (const auto& [source_id, stats_pair] : range->source_stats) {
                    auto& dest = result.range_source_stats[group_idx][source_id];
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
};

struct TabletRangeInfo {
    TabletRangePB range;
    std::unordered_map<uint32_t, Statistic> rowset_stats;
};

Status get_tablet_split_ranges(const TabletMetadataPtr& tablet_metadata, int32_t split_count,
                               std::vector<TabletRangeInfo>* split_ranges) {
    if (split_count < 2) {
        return Status::InvalidArgument("Invalid split count, it is less than 2");
    }

    std::vector<SegmentSplitInfo> segments;

    for (const auto& rowset : tablet_metadata->rowsets()) {
        if (rowset.segments_size() != rowset.segment_size_size() ||
            rowset.segments_size() != rowset.segment_metas_size()) {
            return Status::InvalidArgument("Segment metadata is inconsistent with segment list");
        }
        for (int32_t i = 0; i < rowset.segments_size(); ++i) {
            SegmentSplitInfo seg;
            seg.source_id = rowset.id();
            const auto& segment_meta = rowset.segment_metas(i);
            RETURN_IF_ERROR(seg.min_key.from_proto(segment_meta.sort_key_min()));
            RETURN_IF_ERROR(seg.max_key.from_proto(segment_meta.sort_key_max()));
            seg.num_rows = segment_meta.num_rows();
            seg.data_size = rowset.segment_size(i);
            segments.push_back(std::move(seg));
        }
    }

    if (segments.empty()) {
        return Status::InvalidArgument("No segments found in tablet metadata");
    }

    // Step 2: Calculate split boundaries with tablet range filtering.
    TabletRange tablet_range;
    RETURN_IF_ERROR(tablet_range.from_proto(tablet_metadata->range()));

    int64_t total_num_rows = 0;
    for (const auto& seg : segments) {
        total_num_rows += seg.num_rows;
    }
    int64_t avg_num_rows = std::max<int64_t>(1, total_num_rows / split_count);

    ASSIGN_OR_RETURN(auto split_result, calculate_range_split_boundaries(segments, split_count, avg_num_rows,
                                                                         /*use_num_rows=*/true,
                                                                         /*track_sources=*/true, &tablet_range));

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

        if (i < static_cast<int32_t>(split_result.range_source_stats.size())) {
            for (const auto& [source_id, stats_pair] : split_result.range_source_stats[i]) {
                auto& rowset_stat = sr.rowset_stats[source_id];
                rowset_stat.num_rows = stats_pair.first;
                rowset_stat.data_size = stats_pair.second;
            }
        }
    }

    if (split_ranges->size() < 2) {
        split_ranges->clear();
        return Status::InvalidArgument("Not enough split ranges available");
    }

    return Status::OK();
}

} // namespace

StatusOr<std::unordered_map<int64_t, MutableTabletMetadataPtr>> split_tablet(
        TabletManager* tablet_manager, const TabletMetadataPtr& old_tablet_metadata,
        const SplittingTabletInfoPB& splitting_tablet, int64_t new_version, const TxnInfoPB& txn_info) {
    if (old_tablet_metadata == nullptr) {
        return Status::InvalidArgument("old tablet metadata is null");
    }
    if (splitting_tablet.new_tablet_ids_size() <= 0) {
        return Status::InvalidArgument("splitting tablet has no new tablet");
    }

    std::unordered_map<int64_t, MutableTabletMetadataPtr> new_metadatas;

    std::vector<TabletRangeInfo> split_ranges;
    Status status = get_tablet_split_ranges(old_tablet_metadata, splitting_tablet.new_tablet_ids_size(), &split_ranges);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to get tablet split ranges, will not split this tablet: " << old_tablet_metadata->id()
                     << ", version: " << old_tablet_metadata->version() << ", txn_id: " << txn_info.txn_id()
                     << ", status: " << status;
        auto new_tablet_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_metadata);
        new_tablet_metadata->set_id(splitting_tablet.new_tablet_ids(0));
        new_tablet_metadata->set_version(new_version);
        new_tablet_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_metadata->set_gtid(txn_info.gtid());
        new_metadatas.emplace(new_tablet_metadata->id(), std::move(new_tablet_metadata));
        return new_metadatas;
    }

    DCHECK(split_ranges.size() == splitting_tablet.new_tablet_ids_size());
    for (int32_t i = 0; i < splitting_tablet.new_tablet_ids_size(); ++i) {
        auto new_tablet_new_metadata = std::make_shared<TabletMetadataPB>(*old_tablet_metadata);
        new_tablet_new_metadata->set_id(splitting_tablet.new_tablet_ids(i));
        new_tablet_new_metadata->set_version(new_version);
        new_tablet_new_metadata->set_commit_time(txn_info.commit_time());
        new_tablet_new_metadata->set_gtid(txn_info.gtid());
        new_tablet_new_metadata->mutable_range()->CopyFrom(split_ranges[i].range);
        tablet_reshard_helper::set_all_data_files_shared(new_tablet_new_metadata.get());

        for (auto& rowset_metadata : *new_tablet_new_metadata->mutable_rowsets()) {
            RETURN_IF_ERROR(tablet_reshard_helper::update_rowset_range(&rowset_metadata, split_ranges[i].range));
            const auto it = split_ranges[i].rowset_stats.find(rowset_metadata.id());
            if (it != split_ranges[i].rowset_stats.end()) {
                rowset_metadata.set_num_rows(it->second.num_rows);
                rowset_metadata.set_data_size(it->second.data_size);
            } else {
                rowset_metadata.set_num_rows(0);
                rowset_metadata.set_data_size(0);
            }
        }

        new_metadatas.emplace(new_tablet_new_metadata->id(), std::move(new_tablet_new_metadata));
    }

    return new_metadatas;
}

} // namespace starrocks::lake
