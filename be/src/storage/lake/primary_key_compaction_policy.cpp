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

#include <unordered_set>

#include "common/config_compaction_fwd.h"
#include "common/config_storage_fwd.h"
#include "gutil/strings/join.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/update_manager.h"

namespace starrocks::lake {

int64_t max_sparse_chain_depth_for_rowset(const RowsetMetadataPB& rowset, const DeltaColumnGroupMetadataPB& dcg_meta) {
    const auto& dcgs = dcg_meta.dcgs();
    if (dcgs.empty()) {
        return 0;
    }
    int64_t max_depth = 0;
    // A rowset reserves a contiguous rssid range starting at rowset.id(); probe each segment's rssid.
    // Fall back to the base rssid for legacy rowsets that carry no segment_metas.
    int nseg = rowset.segment_metas_size();
    if (nseg == 0) {
        nseg = 1;
    }
    for (int pos = 0; pos < nseg; ++pos) {
        const uint32_t rssid = get_rssid(rowset, pos);
        auto it = dcgs.find(rssid);
        if (it == dcgs.end()) {
            continue;
        }
        const DeltaColumnGroupVerPB& dcg = it->second;
        int64_t depth = 0;
        for (int i = 0; i < dcg.column_files_size(); ++i) {
            // An absent file_kinds slot is DENSE_COLS; only SPARSE_PERCOL files form the overlay chain.
            const DeltaColumnFileKindPB kind = i < dcg.file_kinds_size() ? dcg.file_kinds(i) : DENSE_COLS;
            if (kind == SPARSE_PERCOL) {
                ++depth;
            }
        }
        // Inline patches are sparse overlays on the same axis as `.spcols` files for convergence.
        depth += dcg.inline_patches_size();
        max_depth = std::max(max_depth, depth);
    }
    return max_depth;
}

double RowsetCandidate::io_count() const {
    int64_t large_rowset_threshold = config::lake_compaction_max_rowset_size;

    // SDCG: a deep sparse-overlay chain costs ~one extra file read per layer even on a well-compacted
    // rowset, so fold it into the rowset's compaction priority. calculate_score() divides io_count() by
    // read_bytes()/MB, so pre-multiply by that factor to make the chain contribute an *un-normalized*
    // score addend (== sdcg_chain_score_contribution(depth)) regardless of the rowset's byte size --
    // a large base rowset's chain would otherwise be diluted to a negligible score and never rise above
    // other levels in pick_max_level. This only raises selection PRIORITY/ordering; convergence is still
    // *guaranteed* by the force-include step in pick_rowset_indexes even if this never makes it win a
    // level. Added on every return path so the priority is consistent across rowset shapes.
    const double sdcg_io = sdcg_chain_score_contribution(sparse_chain_depth) * read_bytes() / (1024.0 * 1024.0);

    // For non-overlapped rowsets that are already large enough, return 0
    // to indicate they don't need compaction. The only exception is if they have deletes,
    // in which case we still want to consider compacting them to reclaim space.
    if (!rowset_meta_ptr->overlapped() && stat.num_dels == 0) {
        int64_t rowset_size = static_cast<int64_t>(rowset_meta_ptr->data_size());
        if (rowset_size >= large_rowset_threshold) {
            // Already a large, well-compacted rowset with no deletes - zero priority,
            // UNLESS it carries an over-deep sparse chain that background convergence must rewrite.
            return sdcg_io;
        }
    }

    double cnt = 1;
    if (rowset_meta_ptr->overlapped()) {
        int segments_size = rowset_meta_ptr->segment_metas_size();
        int segment_size_cnt = 0;
        for (int i = 0; i < segments_size; i++) {
            if (rowset_meta_ptr->segment_metas(i).has_size()) {
                segment_size_cnt++;
            }
        }
        if (segments_size == 0) {
            cnt = 1;
        } else if (segment_size_cnt == 0) {
            // No segment_size info, fall back to counting all segments
            cnt = segments_size;
        } else {
            // Count only segments smaller than the large segment threshold
            int effective_count = 0;
            for (int i = 0; i < segments_size; i++) {
                const auto& segment_meta = rowset_meta_ptr->segment_metas(i);
                if (segment_meta.has_size() && static_cast<int64_t>(segment_meta.size()) < large_rowset_threshold) {
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
    return cnt + sdcg_io;
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
    // SDCG: an over-deep sparse-overlay chain justifies compaction on its own, even when the raw
    // (effective) segment count is below the min-input threshold -- background convergence must
    // materialize the chain regardless. Gate on a non-empty DCG map so non-SDCG tablets are untouched.
    const auto& dcg_meta = tablet_metadata->dcg_meta();
    if (!dcg_meta.dcgs().empty()) {
        for (const auto& rowset : tablet_metadata->rowsets()) {
            if (max_sparse_chain_depth_for_rowset(rowset, dcg_meta) >= SDCG_COMPACTION_TRIGGER_DEPTH) {
                return true;
            }
        }
    }

    int64_t total_segment_cnt = 0;
    int64_t large_rowset_threshold = config::lake_compaction_max_rowset_size;
    for (const auto& rowset : tablet_metadata->rowsets()) {
        if (!rowset.overlapped()) {
            // Large non-overlapped rowsets are already well-compacted, skip them
            if (rowset.data_size() >= large_rowset_threshold) {
                continue;
            }
            total_segment_cnt += 1;
        } else if (rowset.segment_metas_size() == 0) {
            // No segments in the rowset, count as 1
            total_segment_cnt += 1;
        } else {
            int segment_size_cnt = 0;
            for (const auto& segment_meta : rowset.segment_metas()) {
                if (segment_meta.has_size()) {
                    segment_size_cnt++;
                }
            }
            if (segment_size_cnt == 0) {
                // No segment_size info, fall back to counting all segments
                total_segment_cnt += rowset.segment_metas_size();
                continue;
            }
            // Count only segments smaller than the large segment threshold
            int64_t rowset_effective_count = 0;
            for (const auto& segment_meta : rowset.segment_metas()) {
                if (segment_meta.has_size() && static_cast<int64_t>(segment_meta.size()) < large_rowset_threshold) {
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
    // SDCG: precompute the sparse-overlay chain depth per rowset (column-agnostic), so it both raises
    // the candidate's compaction score (via io_count) and lets us force-include over-deep chains below.
    const auto& dcg_meta = tablet_metadata->dcg_meta();
    const bool has_dcg = !dcg_meta.dcgs().empty();
    std::vector<int64_t> chain_depths(tablet_metadata->rowsets_size(), 0);
    bool any_deep_chain = false;
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
        if (has_dcg) {
            chain_depths[i] = max_sparse_chain_depth_for_rowset(rowset_pb, dcg_meta);
            if (chain_depths[i] >= SDCG_COMPACTION_TRIGGER_DEPTH) {
                any_deep_chain = true;
            }
        }
        rowset_vec.emplace_back(&rowset_pb, stat, i, chain_depths[i]);
    }
    // 2. pick largest score level
    ASSIGN_OR_RETURN(auto pick_level_ptr, pick_max_level(rowset_vec));
    // Track which rowsets the size-tiered selection picked, so the SDCG force-include step below does
    // not double-add them. A null level means size-tiered found nothing; deep chains may still need
    // convergence, so we fall through to the force-include rather than returning early.
    std::unordered_set<int64_t> picked_set;
    if (pick_level_ptr == nullptr && !any_deep_chain) {
        return rowset_indexes;
    }

    // 3. pick input rowsets from level
    size_t cur_compaction_result_bytes = 0;
    bool reach_max_input_per_compaction = false;
    while (pick_level_ptr != nullptr && !pick_level_ptr->rowsets.empty()) {
        const auto& rowset_candidate = pick_level_ptr->rowsets.top();
        cur_compaction_result_bytes += rowset_candidate.read_bytes();
        rowset_indexes.push_back(rowset_candidate.rowset_index);
        picked_set.insert(rowset_candidate.rowset_index);
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
    if (is_real_time && !reach_max_input_per_compaction && pick_level_ptr != nullptr) {
        for (int i = 0; i < pick_level_ptr->other_level_rowsets.size(); i++) {
            const auto& rowset_candidate = pick_level_ptr->other_level_rowsets[i];
            cur_compaction_result_bytes += rowset_candidate.read_bytes();
            rowset_indexes.push_back(rowset_candidate.rowset_index);
            picked_set.insert(rowset_candidate.rowset_index);
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

    // 4. SDCG force-include: a rowset whose sparse-overlay chain has grown past the trigger MUST be
    // compacted to converge it, even if byte-normalized size-tiered scoring buried it below other
    // levels (e.g. a large well-compacted base rowset that only accumulates `.spcols` overlays). The
    // size-tiered score nudges such rowsets up but cannot guarantee selection in a mixed workload;
    // this guarantees it. Bounded by lake_pk_compaction_max_input_rowsets so the convergence
    // compaction stays sized like any other. Only active on SDCG tablets (any_deep_chain).
    if (any_deep_chain) {
        for (int i = 0, sz = tablet_metadata->rowsets_size(); i < sz; i++) {
            if (chain_depths[i] < SDCG_COMPACTION_TRIGGER_DEPTH) {
                continue;
            }
            if (picked_set.count(i) > 0) {
                continue;
            }
            if (rowset_indexes.size() >= static_cast<size_t>(config::lake_pk_compaction_max_input_rowsets)) {
                break;
            }
            rowset_indexes.push_back(i);
            picked_set.insert(i);
            if (has_dels != nullptr) {
                const auto& rs = tablet_metadata->rowsets(i);
                const bool has_del =
                        rs.has_num_dels() ? rs.num_dels() > 0 : mgr->get_rowset_num_deletes(*tablet_metadata, rs) > 0;
                has_dels->push_back(has_del);
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
