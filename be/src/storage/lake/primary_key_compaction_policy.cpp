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

#include "storage/lake/primary_key_compaction_policy.h"

#include "gutil/strings/join.h"
#include "storage/lake/update_manager.h"

namespace starrocks::lake {

StatusOr<std::unique_ptr<PKSizeTieredLevel>> PrimaryCompactionPolicy::pick_max_level(
        std::vector<RowsetCandidate>& rowsets) {
    int64_t max_level_size =
            config::size_tiered_min_level_size * pow(config::size_tiered_level_multiple, config::size_tiered_level_num);

    if (rowsets.empty()) {
        return nullptr;
    }
    // sort rowset by bytes
    std::sort(rowsets.begin(), rowsets.end(),
              [](const RowsetCandidate& r1, const RowsetCandidate& r2) { return r1.read_bytes() > r2.read_bytes(); });

    std::priority_queue<PKSizeTieredLevel> order_levels;
    // current level rowsets
    std::vector<RowsetCandidate> current_level_rowsets;
    const int64_t level_multiple = config::size_tiered_level_multiple;
    int64_t level_size = -1;
    for (const auto& rowset : rowsets) {
        int64_t rowset_size = rowset.read_bytes() > 0 ? rowset.read_bytes() : 1;
        if (level_size == -1) {
            level_size = rowset_size < max_level_size ? rowset_size : max_level_size;
        }

        // When calculate score, we don't need to distribute rowsets into different levels.
        if (config::enable_pk_size_tiered_compaction_strategy && level_size > config::size_tiered_min_level_size &&
            rowset_size < level_size && ((double)level_size / (double)rowset_size) > (double)(level_multiple - 1)) {
            // Meet next level rowset
            if (!current_level_rowsets.empty()) {
                order_levels.emplace(current_level_rowsets, level_size);
            }
            current_level_rowsets.clear();
            level_size = rowset_size < max_level_size ? rowset_size : max_level_size;
        }

        current_level_rowsets.emplace_back(rowset);
    }

    if (!current_level_rowsets.empty()) {
        order_levels.emplace(current_level_rowsets, level_size);
    }

    auto top_level_ptr = std::make_unique<PKSizeTieredLevel>(order_levels.top());
    int32_t compaction_level = 1;
    order_levels.pop();
    // When largest score level only have one rowset (without segment overlapped), merge with second larger score level.
    if (top_level_ptr->rowsets.size() == 1 && !top_level_ptr->rowsets.top().multi_segment_with_overlapped() &&
        !order_levels.empty()) {
        auto second_level_ptr = std::make_unique<PKSizeTieredLevel>(order_levels.top());
        top_level_ptr->merge_level(*second_level_ptr);
        order_levels.pop();
        compaction_level++;
    }

    int32_t max_compaction_levels = config::size_tiered_max_compaction_level;
    while (!order_levels.empty() && compaction_level <= max_compaction_levels) {
        auto next_level_ptr = std::make_unique<PKSizeTieredLevel>(order_levels.top());
        order_levels.pop();
        if (next_level_ptr->get_compact_level() < top_level_ptr->get_compact_level()) {
            top_level_ptr->add_other_level_rowsets(*next_level_ptr);
            compaction_level++;
        }
    }
    return top_level_ptr;
}

StatusOr<std::vector<RowsetPtr>> PrimaryCompactionPolicy::pick_rowsets() {
    return pick_rowsets(_tablet_metadata, nullptr);
}

// Return true if segment number meet the requirement of min input
bool min_input_segment_check(const std::shared_ptr<const TabletMetadataPB>& tablet_metadata) {
    int64_t total_segment_cnt = 0;
    int64_t large_segment_threshold =
            static_cast<int64_t>(config::max_segment_file_size * config::lake_compaction_skip_large_segment_ratio);
    for (int i = 0; i < tablet_metadata->rowsets_size(); i++) {
        const auto& rowset = tablet_metadata->rowsets(i);
        if (!rowset.overlapped()) {
            total_segment_cnt += 1;
        } else if (rowset.segments_size() == 0) {
            // No segments in the rowset, count as 1 (consistent with calc_effective_segment_count)
            total_segment_cnt += 1;
        } else if (rowset.segment_size_size() == 0) {
            // No segment_size info, fall back to counting all segments
            total_segment_cnt += rowset.segments_size();
        } else if (!config::enable_lake_compaction_skip_large_segment) {
            total_segment_cnt += rowset.segments_size();
        } else {
            // Count only segments smaller than the large segment threshold
            int64_t rowset_effective_count = 0;
            for (int j = 0; j < rowset.segment_size_size(); j++) {
                if (static_cast<int64_t>(rowset.segment_size(j)) < large_segment_threshold) {
                    rowset_effective_count++;
                }
            }
            // At least count 1 for non-empty rowset
            if (rowset_effective_count == 0) {
                rowset_effective_count = 1;
            }
            total_segment_cnt += rowset_effective_count;
        }
        if (total_segment_cnt >= config::lake_pk_compaction_min_input_segments) {
            // Return when requirement meet
            return true;
        }
    }
    return false;
}

StatusOr<std::vector<int64_t>> PrimaryCompactionPolicy::pick_rowset_indexes(
        const std::shared_ptr<const TabletMetadataPB>& tablet_metadata, std::vector<bool>* has_dels) {
    bool is_real_time = is_real_time_compaction_strategy(tablet_metadata);
    UpdateManager* mgr = _tablet_mgr->update_mgr();
    std::vector<int64_t> rowset_indexes;
    if (!min_input_segment_check(tablet_metadata)) {
        // When the number of segments cannot meet the requirement
        // 1. Compaction score will be zero.
        // 2. None of rowset will be picked.
        return rowset_indexes;
    }
    std::vector<RowsetCandidate> rowset_vec;
    const auto tablet_id = tablet_metadata->id();
    const auto tablet_version = tablet_metadata->version();
    const int64_t compaction_data_size_threshold =
            static_cast<int64_t>((double)_get_data_size(tablet_metadata) * config::update_compaction_ratio_threshold);
    // 1. generate rowset candidate vector
    for (int i = 0, sz = tablet_metadata->rowsets_size(); i < sz; i++) {
        const RowsetMetadataPB& rowset_pb = tablet_metadata->rowsets(i);
        RowsetStat stat;
        stat.num_rows = rowset_pb.num_rows();
        stat.bytes = rowset_pb.data_size();
        if (rowset_pb.has_num_dels()) {
            stat.num_dels = rowset_pb.num_dels();
        } else {
            stat.num_dels = mgr->get_rowset_num_deletes(tablet_id, tablet_version, rowset_pb);
        }
        rowset_vec.emplace_back(&rowset_pb, stat, i);
    }
    // 2. pick largest score level
    ASSIGN_OR_RETURN(auto pick_level_ptr, pick_max_level(rowset_vec));
    if (pick_level_ptr == nullptr) {
        return rowset_indexes;
    }

    // 3. pick input rowsets from level
    size_t cur_compaction_result_bytes = 0;
    bool reach_max_input_per_compaction = false;
    while (!pick_level_ptr->rowsets.empty()) {
        const auto& rowset_candidate = pick_level_ptr->rowsets.top();
        cur_compaction_result_bytes += rowset_candidate.read_bytes();
        rowset_indexes.push_back(rowset_candidate.rowset_index);
        if (has_dels != nullptr) {
            has_dels->push_back(rowset_candidate.delete_bytes() > 0);
        }

        if (cur_compaction_result_bytes >
            std::max(config::update_compaction_result_bytes, compaction_data_size_threshold)) {
            reach_max_input_per_compaction = true;
            break;
        }
        if (rowset_indexes.size() >= config::lake_pk_compaction_max_input_rowsets) {
            reach_max_input_per_compaction = true;
            break;
        }
        pick_level_ptr->rowsets.pop();
    }
    if (is_real_time && !reach_max_input_per_compaction) {
        for (int i = 0; i < pick_level_ptr->other_level_rowsets.size(); i++) {
            const auto& rowset_candidate = pick_level_ptr->other_level_rowsets[i];
            cur_compaction_result_bytes += rowset_candidate.read_bytes();
            rowset_indexes.push_back(rowset_candidate.rowset_index);
            if (has_dels != nullptr) {
                has_dels->push_back(rowset_candidate.delete_bytes() > 0);
            }

            if (cur_compaction_result_bytes >
                std::max(config::update_compaction_result_bytes, compaction_data_size_threshold)) {
                break;
            }
            if (rowset_indexes.size() >= config::lake_pk_compaction_max_input_rowsets) {
                reach_max_input_per_compaction = true;
                break;
            }
        }
    }

    return rowset_indexes;
}

StatusOr<std::vector<RowsetPtr>> PrimaryCompactionPolicy::pick_rowsets(
        const std::shared_ptr<const TabletMetadataPB>& tablet_metadata, std::vector<bool>* has_dels) {
    std::vector<RowsetPtr> input_rowsets;
    ASSIGN_OR_RETURN(auto rowset_indexes, pick_rowset_indexes(tablet_metadata, has_dels));
    input_rowsets.reserve(rowset_indexes.size());
    for (auto rowset_index : rowset_indexes) {
        input_rowsets.emplace_back(
                std::make_shared<Rowset>(_tablet_mgr, tablet_metadata, rowset_index, 0 /* compaction_segment_limit */));
    }
    VLOG(2) << strings::Substitute(
            "lake PrimaryCompactionPolicy pick_rowsets tabletid:$0 version:$1 inputs:$2", tablet_metadata->id(),
            tablet_metadata->version(),
            JoinMapped(
                    input_rowsets, [&](const RowsetPtr& rowset) -> std::string { return std::to_string(rowset->id()); },
                    "|"));
    return input_rowsets;
}

int64_t PrimaryCompactionPolicy::_get_data_size(const std::shared_ptr<const TabletMetadataPB>& tablet_metadata) {
    int64_t size = 0;
    for (const auto& rowset : tablet_metadata->rowsets()) {
        size += rowset.data_size();
    }
    return size;
}

} // namespace starrocks::lake
