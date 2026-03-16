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
#include "storage/variant_tuple.h"

namespace starrocks::lake {

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

    struct SegmentInfo {
        uint32_t rowset_id;
        VariantTuple min;
        VariantTuple max;
        Statistic stat;
    };

    std::vector<SegmentInfo> segment_infos;
    for (const auto& rowset : tablet_metadata->rowsets()) {
        if (rowset.segments_size() != rowset.segment_size_size() ||
            rowset.segments_size() != rowset.segment_metas_size()) {
            return Status::InvalidArgument("Segment metadata is inconsistent with segment list");
        }
        for (int32_t i = 0; i < rowset.segments_size(); ++i) {
            auto& segment_info = segment_infos.emplace_back();
            segment_info.rowset_id = rowset.id();
            const auto& segment_meta = rowset.segment_metas(i);
            RETURN_IF_ERROR(segment_info.min.from_proto(segment_meta.sort_key_min()));
            RETURN_IF_ERROR(segment_info.max.from_proto(segment_meta.sort_key_max()));
            segment_info.stat.num_rows = segment_meta.num_rows();
            segment_info.stat.data_size = rowset.segment_size(i);
        }
    }

    if (segment_infos.empty()) {
        return Status::InvalidArgument("No segments found in tablet metadata");
    }

    auto comparator = [](const VariantTuple* key1, const VariantTuple* key2) { return key1->compare(*key2) < 0; };
    std::set<const VariantTuple*, decltype(comparator)> ordered_range_boundaries;
    for (const auto& segment_info : segment_infos) {
        ordered_range_boundaries.insert(&segment_info.min);
        ordered_range_boundaries.insert(&segment_info.max);
    }

    struct RangeInfo {
        VariantTuple min;
        VariantTuple max;
        Statistic stat;
        std::unordered_map<uint32_t, Statistic> rowset_stats;
    };

    std::vector<RangeInfo> ordered_ranges;
    ordered_ranges.reserve(ordered_range_boundaries.size());
    const VariantTuple* last_boundary = nullptr;
    for (const auto* range_boundary : ordered_range_boundaries) {
        if (last_boundary != nullptr) {
            auto& range_info = ordered_ranges.emplace_back();
            range_info.min = *last_boundary;
            range_info.max = *range_boundary;
            range_info.stat.num_rows = 0;
            range_info.stat.data_size = 0;
        }
        last_boundary = range_boundary;
    }

    if (ordered_ranges.empty()) {
        return Status::InvalidArgument("No split ranges available");
    }

    for (const auto& segment_info : segment_infos) {
        std::vector<RangeInfo*> overlapping_ranges;
        for (auto& range_info : ordered_ranges) {
            if (&range_info != &ordered_ranges.back()) {
                if (!(range_info.max.compare(segment_info.min) <= 0 || range_info.min.compare(segment_info.max) >= 0)) {
                    overlapping_ranges.push_back(&range_info);
                }
            } else {
                if (!(range_info.max.compare(segment_info.min) < 0 || range_info.min.compare(segment_info.max) > 0)) {
                    overlapping_ranges.push_back(&range_info);
                }
            }
        }

        if (overlapping_ranges.empty()) {
            continue;
        }

        const auto split_num_rows = segment_info.stat.num_rows / overlapping_ranges.size();
        const auto remain_num_rows = segment_info.stat.num_rows % overlapping_ranges.size();
        const auto split_data_size = segment_info.stat.data_size / overlapping_ranges.size();
        const auto remain_data_size = segment_info.stat.data_size % overlapping_ranges.size();
        for (size_t i = 0; i < overlapping_ranges.size(); ++i) {
            auto delta_num_rows = split_num_rows;
            auto delta_data_size = split_data_size;
            if (i < remain_num_rows) {
                ++delta_num_rows;
            }
            if (i < remain_data_size) {
                ++delta_data_size;
            }

            auto* range_info = overlapping_ranges[i];
            range_info->stat.num_rows += delta_num_rows;
            range_info->stat.data_size += delta_data_size;

            auto& rowset_stat = range_info->rowset_stats[segment_info.rowset_id];
            rowset_stat.num_rows += delta_num_rows;
            rowset_stat.data_size += delta_data_size;
        }
    }

    TabletRange tablet_range;
    RETURN_IF_ERROR(tablet_range.from_proto(tablet_metadata->range()));
    std::vector<const RangeInfo*> tablet_overlapped_ranges;
    tablet_overlapped_ranges.reserve(ordered_ranges.size());
    int64_t total_num_rows = 0;
    size_t non_empty_ranges = 0;
    for (const auto& range_info : ordered_ranges) {
        if (tablet_range.less_than(range_info.min) || tablet_range.greater_than(range_info.max)) {
            continue;
        }
        tablet_overlapped_ranges.push_back(&range_info);
        total_num_rows += range_info.stat.num_rows;
        if (range_info.stat.num_rows > 0) {
            ++non_empty_ranges;
        }
    }

    if (non_empty_ranges < static_cast<size_t>(split_count)) {
        return Status::InvalidArgument("Not enough split ranges available");
    }

    DCHECK(split_ranges->empty());
    split_ranges->reserve(split_count);
    const int64_t avg_num_rows = std::max<int64_t>(1, total_num_rows / split_count);
    int64_t acc_num_rows = 0;
    std::unordered_map<uint32_t, Statistic> acc_rowset_stats;
    last_boundary = nullptr;
    size_t remaining_non_empty_ranges = non_empty_ranges;
    for (size_t i = 0; i < tablet_overlapped_ranges.size(); ++i) {
        const auto* range_info = tablet_overlapped_ranges[i];
        const bool is_non_empty = range_info->stat.num_rows > 0;
        acc_num_rows += range_info->stat.num_rows;
        for (const auto& [rowset_id, stat] : range_info->rowset_stats) {
            auto& rowset_stat = acc_rowset_stats[rowset_id];
            rowset_stat.num_rows += stat.num_rows;
            rowset_stat.data_size += stat.data_size;
        }

        const auto remaining_splits = static_cast<size_t>(split_count) - split_ranges->size();
        if (is_non_empty && remaining_splits > 0 &&
            (acc_num_rows >= avg_num_rows || remaining_non_empty_ranges <= remaining_splits)) {
            const VariantTuple* boundary = &range_info->max;
            for (size_t j = i + 1; j < tablet_overlapped_ranges.size(); ++j) {
                const auto* next_range = tablet_overlapped_ranges[j];
                if (next_range->stat.num_rows > 0 || !tablet_range.strictly_contains(next_range->max)) {
                    break;
                }
                boundary = &next_range->max;
            }

            if ((last_boundary == nullptr || boundary->compare(*last_boundary) > 0) &&
                tablet_range.strictly_contains(*boundary)) {
                auto& split_range = split_ranges->emplace_back();
                if (last_boundary == nullptr) {
                    split_range.range = tablet_metadata->range();
                } else {
                    last_boundary->to_proto(split_range.range.mutable_lower_bound());
                    split_range.range.set_lower_bound_included(true);
                }

                boundary->to_proto(split_range.range.mutable_upper_bound());
                split_range.range.set_upper_bound_included(false);

                for (const auto& [rowset_id, stat] : acc_rowset_stats) {
                    auto& rowset_stat = split_range.rowset_stats[rowset_id];
                    rowset_stat.num_rows = stat.num_rows;
                    rowset_stat.data_size = stat.data_size;
                }

                acc_num_rows = 0;
                acc_rowset_stats.clear();
                last_boundary = boundary;
            }
        }

        if (is_non_empty) {
            --remaining_non_empty_ranges;
        }
    }

    if (split_ranges->size() == static_cast<size_t>(split_count)) {
        auto& split_range = split_ranges->back();
        if (tablet_metadata->range().has_upper_bound()) {
            split_range.range.mutable_upper_bound()->CopyFrom(tablet_metadata->range().upper_bound());
            split_range.range.set_upper_bound_included(tablet_metadata->range().upper_bound_included());
        } else {
            split_range.range.clear_upper_bound();
            split_range.range.clear_upper_bound_included();
        }
        for (const auto& [rowset_id, stat] : acc_rowset_stats) {
            auto& rowset_stat = split_range.rowset_stats[rowset_id];
            rowset_stat.num_rows += stat.num_rows;
            rowset_stat.data_size += stat.data_size;
        }
    } else if (split_ranges->size() + 1 == static_cast<size_t>(split_count) && acc_num_rows > 0) {
        auto& split_range = split_ranges->emplace_back();
        split_range.range = tablet_metadata->range();
        split_range.range.mutable_lower_bound()->CopyFrom((*split_ranges)[split_count - 2].range.upper_bound());
        split_range.range.set_lower_bound_included(true);
        for (const auto& [rowset_id, stat] : acc_rowset_stats) {
            auto& rowset_stat = split_range.rowset_stats[rowset_id];
            rowset_stat.num_rows = stat.num_rows;
            rowset_stat.data_size = stat.data_size;
        }
    } else {
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
