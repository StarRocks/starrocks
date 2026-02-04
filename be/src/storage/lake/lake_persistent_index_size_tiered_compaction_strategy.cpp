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

#include "storage/lake/lake_persistent_index_size_tiered_compaction_strategy.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "base/utility/defer_op.h"
#include "common/config.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/persistent_index_sstable.h"
#include "storage/lake/tablet_manager.h"

namespace starrocks::lake {

StatusOr<CompactionCandidateResult> LakePersistentIndexSizeTieredCompactionStrategy::pick_compaction_candidates(
        const PersistentIndexSstableMetaPB& sstable_meta) {
    CompactionCandidateResult result;
    result.candidate_filesets.clear();

    if (sstable_meta.sstables_size() == 0) {
        return result;
    }

    // Group sstables by fileset_id
    // Key: fileset_id, Value: vector of sstable indices and total size
    struct FilesetInfo {
        std::vector<int> sstable_indices;
        int64_t total_size = 0;
        UniqueId fileset_id;
    };

    std::vector<FilesetInfo> filesets;
    std::unordered_map<UniqueId, size_t> fileset_id_to_index;
    UniqueId base_level_fileset_id; // fileset id of base level (the first fileset)
    // fileset id of active level (the last fileset)
    UniqueId active_fileset_id =
            sstable_meta.sstables(sstable_meta.sstables_size() - 1).has_fileset_id()
                    ? UniqueId(sstable_meta.sstables(sstable_meta.sstables_size() - 1).fileset_id())
                    : UniqueId::gen_uid();

    for (int i = 0; i < sstable_meta.sstables_size(); ++i) {
        const auto& sstable_pb = sstable_meta.sstables(i);
        // Every sstable without an explicit fileset_id will be treated as belonging to a different fileset.
        // That is because these sstable lack of key range info, and we cannot group them correctly.
        UniqueId fileset_id = sstable_pb.has_fileset_id() ? sstable_pb.fileset_id() : UniqueId::gen_uid();
        if (fileset_id == active_fileset_id) {
            // Active fileset should not be compacted
            continue;
        }
        if (i == 0) {
            base_level_fileset_id = fileset_id;
        }

        auto it = fileset_id_to_index.find(fileset_id);
        if (it == fileset_id_to_index.end()) {
            FilesetInfo info;
            info.fileset_id = fileset_id;
            info.sstable_indices.push_back(i);
            info.total_size = sstable_pb.filesize();
            fileset_id_to_index[fileset_id] = filesets.size();
            filesets.push_back(std::move(info));
        } else {
            size_t index = it->second;
            if (filesets[index].sstable_indices.back() != i - 1) {
                // Should not happen, filesets should be continuous in sstable list
                LOG(ERROR) << fmt::format("Inconsistent fileset_id in sstables: {}", sstable_meta.ShortDebugString());
                return Status::InternalError("inconsistent fileset_id in sstables");
            }
            filesets[index].sstable_indices.push_back(i);
            filesets[index].total_size += sstable_pb.filesize();
        }
    }

    // If only one fileset or no filesets, no compaction needed
    if (filesets.size() <= 1) {
        return result;
    }

    // Apply size-tiered compaction strategy
    // Similar to SizeTieredCompactionPolicy but for filesets
    const int64_t level_multiplier = config::pk_index_size_tiered_level_multiplier;
    const int64_t max_level_size = config::pk_index_size_tiered_min_level_size *
                                   std::pow(level_multiplier, config::pk_index_size_tiered_max_level);
    const int64_t min_compaction_filesets = 2;

    auto cal_compaction_score = [&](int64_t fileset_num, int64_t level_size) -> double {
        if (fileset_num <= 0) {
            // Not need to compact single fileset
            return 0.0;
        }
        // Read amplification score: more filesets means more files to scan during queries
        // Formula: base_score + read_amplification_penalty
        //        = fileset_num + (fileset_num - 1) * 2
        //        = 3 * fileset_num - 2
        double score = 3.0 * fileset_num - 2.0;

        // Level bonus: smaller levels have higher priority to reduce compaction latency
        // Small levels are cheaper to compact and finish faster
        int64_t level_bonus = 0;
        for (int64_t v = level_size; v < max_level_size && level_bonus <= config::pk_index_size_tiered_max_level;
             ++level_bonus) {
            v = v * level_multiplier;
        }
        score += level_bonus;

        return score;
    };

    std::vector<std::unique_ptr<SizeTieredLevel>> order_levels;
    std::set<SizeTieredLevel*, LevelComparator> priority_levels;
    std::vector<size_t> transient_filesets;
    int64_t level_size = -1;
    int64_t total_size = 0;

    for (size_t i = 0; i < filesets.size(); ++i) {
        int64_t fileset_size = std::max(filesets[i].total_size, static_cast<int64_t>(1));

        if (level_size == -1) {
            level_size = fileset_size < max_level_size ? fileset_size : max_level_size;
            total_size = 0;
        }

        // Check if we need to start a new level
        if (level_size > config::pk_index_size_tiered_min_level_size && fileset_size < level_size &&
            level_size / fileset_size > (level_multiplier - 1)) {
            // Close current level
            if (!transient_filesets.empty()) {
                auto level =
                        std::make_unique<SizeTieredLevel>(transient_filesets, level_size, total_size,
                                                          cal_compaction_score(transient_filesets.size(), level_size));
                priority_levels.emplace(level.get());
                order_levels.emplace_back(std::move(level));
            }

            // Start new level
            transient_filesets.clear();
            level_size = fileset_size < max_level_size ? fileset_size : max_level_size;
            total_size = 0;
        }

        transient_filesets.push_back(i);
        total_size += fileset_size;
    }

    // Don't forget the last level
    if (!transient_filesets.empty()) {
        auto level = std::make_unique<SizeTieredLevel>(transient_filesets, level_size, total_size,
                                                       cal_compaction_score(transient_filesets.size(), level_size));
        priority_levels.emplace(level.get());
        order_levels.emplace_back(std::move(level));
    }

    // Pick the best level for compaction
    if (priority_levels.empty()) {
        return result;
    }

    SizeTieredLevel* selected_level = *priority_levels.begin();
    if (selected_level->fileset_indexes.size() < min_compaction_filesets) {
        return result;
    }

    // Build result: for each selected fileset, add all its sstables
    uint64_t max_max_rss_rowid = 0;
    bool merge_base_level = false;
    for (size_t fileset_idx : selected_level->fileset_indexes) {
        const FilesetInfo& fileset = filesets[fileset_idx];
        std::vector<PersistentIndexSstablePB> fileset_sstables;

        for (int sstable_idx : fileset.sstable_indices) {
            fileset_sstables.push_back(sstable_meta.sstables(sstable_idx));
            max_max_rss_rowid = std::max(max_max_rss_rowid, sstable_meta.sstables(sstable_idx).max_rss_rowid());
        }
        if (fileset.fileset_id == base_level_fileset_id) {
            merge_base_level = true;
        }

        result.candidate_filesets.push_back(std::move(fileset_sstables));
    }
    // Limit max sstable filesets that can do merge, to avoid cost too much memory.
    const int32_t max_limit = config::lake_pk_index_sst_max_compaction_versions;
    if (result.candidate_filesets.size() > max_limit) {
        result.candidate_filesets.resize(max_limit);
    }
    result.merge_base_level = merge_base_level;
    result.max_max_rss_rowid = max_max_rss_rowid;

    return result;
}

} // namespace starrocks::lake