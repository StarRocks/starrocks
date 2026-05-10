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

#include "common/config_compaction_fwd.h"
#include "common/config_storage_fwd.h"
#include "gutil/strings/join.h"
#include "storage/lake/update_manager.h"

namespace starrocks::lake {

double RowsetCandidate::io_count() const {
    int64_t large_rowset_threshold = config::lake_compaction_max_rowset_size;

    // For non-overlapped rowsets that are already large enough, return 0
    // to indicate they don't need compaction. The only exception is if they have deletes,
    // in which case we still want to consider compacting them to reclaim space.
    if (!rowset_meta_ptr->overlapped() && stat.num_dels == 0) {
        int64_t rowset_size = static_cast<int64_t>(rowset_meta_ptr->data_size());
        if (rowset_size >= large_rowset_threshold) {
            // Already a large, well-compacted rowset with no deletes - zero priority
            return 0;
        }
    }

    double cnt = 1;
    if (rowset_meta_ptr->overlapped()) {
        int segments_size = rowset_meta_ptr->segments_size();
        if (segments_size == 0) {
            cnt = 1;
        } else if (rowset_meta_ptr->segment_size_size() == 0) {
            // No segment_size info, fall back to counting all segments
            cnt = segments_size;
        } else {
            // Count only segments smaller than the large segment threshold
            int effective_count = 0;
            for (int i = 0; i < rowset_meta_ptr->segment_size_size(); i++) {
                if (static_cast<int64_t>(rowset_meta_ptr->segment_size(i)) < large_rowset_threshold) {
                    effective_count++;
                }
            }
            cnt = std::max(1, effective_count);
        }
    }
    if (stat.num_dels > 0) {
        // if delvec file exist, that means we need to read segment files and delvec files both
        // And update_compaction_delvec_file_io_ratio control the io amp ratio of delvec files, default is 2.
        // Bigger update_compaction_delvec_file_io_amp_ratio means high priority about merge rowset with delvec files.
        cnt *= config::update_compaction_delvec_file_io_amp_ratio;
    }
    return cnt;
}

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
    int64_t large_rowset_threshold = config::lake_compaction_max_rowset_size;
    for (int i = 0; i < tablet_metadata->rowsets_size(); i++) {
        const auto& rowset = tablet_metadata->rowsets(i);
        if (!rowset.overlapped()) {
            // Large non-overlapped rowsets are already well-compacted, skip them
            if (rowset.data_size() >= large_rowset_threshold) {
                continue;
            }
            total_segment_cnt += 1;
        } else if (rowset.segments_size() == 0) {
            // No segments in the rowset, count as 1
            total_segment_cnt += 1;
        } else if (rowset.segment_size_size() == 0) {
            // No segment_size info, fall back to counting all segments
            total_segment_cnt += rowset.segments_size();
        } else {
            // Count only segments smaller than the large segment threshold
            int64_t rowset_effective_count = 0;
            for (int j = 0; j < rowset.segment_size_size(); j++) {
                if (static_cast<int64_t>(rowset.segment_size(j)) < large_rowset_threshold) {
                    rowset_effective_count++;
                }
            }
            // At least count 1 for non-empty overlapped rowset
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
            stat.num_dels = mgr->get_rowset_num_deletes(*tablet_metadata, rowset_pb);
        }
        rowset_vec.emplace_back(&rowset_pb, stat, i);
    }
    // 2. pick largest score level
    ASSIGN_OR_RETURN(auto pick_level_ptr, pick_max_level(rowset_vec));
    if (pick_level_ptr == nullptr) {
        return rowset_indexes;
    }

    // 2b. Skip the picked level if its compaction score is too low to justify the
    // rewrite cost AND it has no overlapping segments AND no deletes.
    //
    // Background: size-tiered selection always returns the highest-score level, even
    // when no level genuinely needs compaction. On large PK tablets this manifests as
    // pathological "sparse mid-tier base merges": a level with only a few large
    // non-overlapped rowsets (e.g., 4 x 700MB on a 13GB tablet) has very low score
    // (~0.006) but still gets picked because L0 was already drained by prior cumulative
    // compactions, leaving this mid-tier as the only candidate. Each such pick rewrites
    // GBs of data with negligible file-count reduction, dominating write amplification.
    //
    // Levels that contain overlapped (multi-segment) rowsets are always allowed to
    // compact since their inherent IO overhead can only be reduced by compaction.
    // Levels containing deletes are also allowed, since delete vectors must eventually
    // be applied/cleaned up via compaction.
    //
    // PR-1' (#72411 design fix): augment the binary level_score gate with two graduated
    // signals so the gate doesn't get stuck on partitions whose read amplification keeps
    // climbing while compaction work itself is uneconomical:
    //   - benefit/cost ratio: estimated segment-count drop per MB rewritten. If the
    //     ratio is decent (>= lake_pk_compaction_min_benefit_cost_ratio), allow the
    //     compaction even when the level score is below the absolute threshold.
    //   - read-pressure emergency override: if the tablet's overall read pressure
    //     (segment count across all rowsets) exceeds lake_pk_compaction_emergency_score,
    //     bypass the gate entirely. Hot partitions need to keep up regardless of
    //     write-amplification cost.
    if (pick_level_ptr->score < config::lake_pk_compaction_min_level_score) {
        bool has_overlap = false;
        bool has_deletes = false;
        double estimated_benefit_segs = 0.0;
        double estimated_io_mb = 0.0;
        auto rs_copy = pick_level_ptr->rowsets;
        while (!rs_copy.empty()) {
            const auto& r = rs_copy.top();
            if (r.multi_segment_with_overlapped()) has_overlap = true;
            if (r.delete_bytes() > 0) has_deletes = true;
            estimated_benefit_segs += static_cast<double>(r.rowset_meta_ptr->segments_size());
            estimated_io_mb += r.read_bytes() / (1024.0 * 1024.0);
            rs_copy.pop();
        }
        if (!has_overlap && !has_deletes) {
            // Tablet-level read pressure: sum of every rowset's score across all
            // levels (not just the picked one). High pressure means many fragmented
            // rowsets, so even uneconomical compactions are worth running to keep
            // read amplification bounded.
            double tablet_read_pressure = 0.0;
            for (const auto& rc : rowset_vec) {
                tablet_read_pressure += rc.score;
            }
            const bool emergency_override =
                    config::lake_pk_compaction_emergency_score > 0.0 &&
                    tablet_read_pressure >= config::lake_pk_compaction_emergency_score;
            const double benefit_cost_ratio =
                    estimated_benefit_segs / std::max(estimated_io_mb, 1.0);
            const bool ratio_ok =
                    config::lake_pk_compaction_min_benefit_cost_ratio > 0.0 &&
                    benefit_cost_ratio >= config::lake_pk_compaction_min_benefit_cost_ratio;
            if (!emergency_override && !ratio_ok) {
                VLOG(2) << strings::Substitute(
                        "lake PK compaction skipped: tablet=$0 level_score=$1 < threshold=$2 "
                        "tablet_read_pressure=$3 (emergency=$4) "
                        "benefit_cost_ratio=$5 (min_ratio=$6) — sparse mid-tier",
                        tablet_metadata->id(), pick_level_ptr->score,
                        config::lake_pk_compaction_min_level_score,
                        tablet_read_pressure, config::lake_pk_compaction_emergency_score,
                        benefit_cost_ratio, config::lake_pk_compaction_min_benefit_cost_ratio);
                return rowset_indexes; // empty -> no compaction this round
            }
        }
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
