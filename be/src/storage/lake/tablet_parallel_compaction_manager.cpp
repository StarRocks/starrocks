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

#include "storage/lake/tablet_parallel_compaction_manager.h"

#include <algorithm>
#include <set>
#include <sstream>

#include "base/utility/defer_op.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/thread/threadpool.h"
#include "gen_cpp/lake_types.pb.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/filenames.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/memtable_flush_executor.h"
#include "storage/rows_mapper.h"
#include "storage/storage_engine.h"

namespace starrocks::lake {

// ================================================================================
// Helper functions for split_rowsets_into_groups
// ================================================================================

// Check if a group is valid for compaction.
// A group is valid if:
// - It has >= 2 rowsets (can merge multiple rowsets), OR
// - It has 1 overlapped rowset with >= 2 segments (can merge internal segments)
bool TabletParallelCompactionManager::_is_group_valid_for_compaction(const std::vector<RowsetPtr>& group) {
    if (group.size() >= 2) {
        return true;
    }
    if (group.size() == 1) {
        const auto& meta = group[0]->metadata();
        return meta.overlapped() && meta.segments_size() >= 2;
    }
    return false;
}

// Filter out large non-overlapped rowsets that don't need compaction.
// Returns the compactable rowsets and logs the skipped ones.
// Uses lake_compaction_max_rowset_size for filtering large rowsets.
std::vector<RowsetPtr> TabletParallelCompactionManager::_filter_compactable_rowsets(
        int64_t tablet_id, std::vector<RowsetPtr> all_rowsets) {
    std::vector<RowsetPtr> compactable_rowsets;
    std::vector<RowsetPtr> skipped_large_rowsets;
    int64_t max_rowset_size = config::lake_compaction_max_rowset_size;

    for (auto& r : all_rowsets) {
        const auto& meta = r->metadata();
        // A rowset is considered "already well-compacted" if:
        // 1. It's non-overlapped (segments don't overlap with each other)
        // 2. Its size >= lake_compaction_max_rowset_size (already large enough)
        // 3. It has no delete predicate (if it has delete, it needs to be processed)
        bool is_large_non_overlapped =
                !meta.overlapped() && r->data_size() >= max_rowset_size && !meta.has_delete_predicate();
        if (is_large_non_overlapped) {
            skipped_large_rowsets.push_back(std::move(r));
        } else {
            compactable_rowsets.push_back(std::move(r));
        }
    }

    if (!skipped_large_rowsets.empty()) {
        std::vector<uint32_t> skipped_ids;
        int64_t skipped_bytes = 0;
        for (const auto& r : skipped_large_rowsets) {
            skipped_ids.push_back(r->id());
            skipped_bytes += r->data_size();
        }
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " skipped " << skipped_large_rowsets.size()
                << " large non-overlapped rowsets (" << skipped_bytes << " bytes) that don't need compaction, ids=["
                << JoinInts(skipped_ids, ",") << "]";
    }

    return compactable_rowsets;
}

// Calculate statistics about rowsets
TabletParallelCompactionManager::RowsetStats TabletParallelCompactionManager::_calculate_rowset_stats(
        const std::vector<RowsetPtr>& rowsets) {
    RowsetStats stats;
    for (const auto& r : rowsets) {
        const auto& meta = r->metadata();
        stats.total_bytes += r->data_size();
        if (meta.has_delete_predicate()) {
            stats.has_delete_predicate = true;
        }
        // Count effective segments
        if (meta.overlapped()) {
            stats.total_segments += std::max(1, meta.segments_size());
        } else {
            stats.total_segments += 1;
        }
    }
    return stats;
}

// Calculate optimal number of subtasks
TabletParallelCompactionManager::GroupingConfig TabletParallelCompactionManager::_calculate_grouping_config(
        int64_t tablet_id, const std::vector<RowsetPtr>& rowsets, const RowsetStats& stats, int32_t max_parallel,
        int64_t max_bytes) {
    GroupingConfig config;

    // Key constraint: each subtask should have at least 2 segments to be meaningful.
    // So max possible subtasks = total_segments / 2
    int32_t max_subtasks_by_segments = static_cast<int32_t>(stats.total_segments / 2);
    int32_t num_subtasks_by_bytes = static_cast<int32_t>((stats.total_bytes + max_bytes - 1) / max_bytes);

    // Take the minimum of: bytes-based count, max_parallel, and segments-based count
    config.num_subtasks = std::min({num_subtasks_by_bytes, max_parallel, max_subtasks_by_segments});
    config.num_subtasks = std::max(config.num_subtasks, 1);

    // Check if we need to skip some data due to limits
    config.skip_excess_data = num_subtasks_by_bytes > config.num_subtasks;
    if (config.skip_excess_data) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id
                << " data exceeds capacity, will skip excess data for later compaction"
                << ", num_subtasks=" << config.num_subtasks << ", would need by bytes=" << num_subtasks_by_bytes
                << ", max_parallel=" << max_parallel << ", max_by_segments=" << max_subtasks_by_segments;
    }

    // Calculate target bytes per subtask for even distribution
    int64_t processable_bytes =
            config.skip_excess_data ? (static_cast<int64_t>(config.num_subtasks) * max_bytes) : stats.total_bytes;
    config.target_bytes_per_subtask = processable_bytes / config.num_subtasks;

    // Calculate target rowsets per subtask for balanced distribution
    config.target_rowsets_per_subtask = std::max(static_cast<size_t>(2), rowsets.size() / config.num_subtasks);

    // For non-PK tables, if partial segments compaction is enabled, limit segments per subtask
    // to avoid triggering partial compaction within each subtask.
    // This ensures each subtask can be applied atomically without segment-level splitting.
    if (config::enable_lake_compaction_use_partial_segments) {
        config.max_segments_per_subtask = config::max_cumulative_compaction_num_singleton_deltas;
    }

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " planning " << config.num_subtasks
            << " subtasks with target_bytes_per_subtask=" << config.target_bytes_per_subtask
            << " target_rowsets_per_subtask=" << config.target_rowsets_per_subtask
            << " max_segments_per_subtask=" << config.max_segments_per_subtask
            << " (processable_bytes=" << processable_bytes << ")";

    return config;
}

// Group rowsets into subtasks based on the configuration.
// Returns groups of rowsets, where each group will become a subtask.
std::vector<std::vector<RowsetPtr>> TabletParallelCompactionManager::_group_rowsets_into_subtasks(
        int64_t tablet_id, std::vector<RowsetPtr> all_rowsets, const GroupingConfig& config, int64_t max_bytes,
        bool is_pk_table) {
    std::vector<std::vector<RowsetPtr>> valid_groups;
    std::vector<RowsetPtr> current_group;
    int64_t current_bytes = 0;
    int64_t current_segments = 0; // Track segments count for non-PK tables
    size_t skipped_rowsets = 0;
    int64_t skipped_bytes = 0;
    int32_t last_rowset_index = -1;

    // Helper lambda to get segment count for a rowset
    auto get_rowset_segments = [](const RowsetPtr& rowset) -> int64_t {
        const auto& meta = rowset->metadata();
        return std::max(1, meta.segments_size());
    };

    for (size_t rowset_idx = 0; rowset_idx < all_rowsets.size(); rowset_idx++) {
        auto& rowset = all_rowsets[rowset_idx];
        int64_t rowset_bytes = rowset->data_size();
        int64_t rowset_segments = get_rowset_segments(rowset);
        int32_t current_rowset_index = rowset->index();

        // Check if we should skip this rowset due to capacity limits
        if (config.skip_excess_data) {
            // Case 1: We've already finalized all planned groups, skip remaining
            if (static_cast<int32_t>(valid_groups.size()) >= config.num_subtasks) {
                skipped_rowsets++;
                skipped_bytes += rowset_bytes;
                continue;
            }
            // Case 2: We're building the last group and adding would exceed max_bytes
            if (static_cast<int32_t>(valid_groups.size()) == config.num_subtasks - 1 && current_group.size() >= 2 &&
                current_bytes + rowset_bytes > max_bytes) {
                skipped_rowsets++;
                skipped_bytes += rowset_bytes;
                continue;
            }
        }

        // Check for adjacency gap (only for non-PK tables)
        bool has_adjacency_gap = false;
        if (!is_pk_table) {
            has_adjacency_gap =
                    !current_group.empty() && last_rowset_index >= 0 && (current_rowset_index - last_rowset_index) > 1;
        }

        // Determine if we should start a new group
        size_t remaining_rowsets = all_rowsets.size() - rowset_idx;
        bool has_enough_remaining = remaining_rowsets >= 1;
        bool current_group_valid = _is_group_valid_for_compaction(current_group);
        // For PK tables: use greedy strategy - fill up current group to max_bytes before starting new one
        // For non-PK tables: use balanced strategy - distribute evenly using target_bytes_per_subtask
        int64_t bytes_threshold = is_pk_table ? max_bytes : config.target_bytes_per_subtask;
        bool would_exceed_target = current_bytes + rowset_bytes > bytes_threshold;
        bool can_create_more_groups = static_cast<int32_t>(valid_groups.size()) < config.num_subtasks - 1;

        bool should_start_new_group = !current_group.empty() && current_group_valid && would_exceed_target &&
                                      can_create_more_groups && has_enough_remaining;

        // Force new group on adjacency gap
        if (has_adjacency_gap && !current_group.empty()) {
            should_start_new_group = true;
        }

        // For non-PK tables: check if adding this rowset would exceed segment limit.
        // This avoids triggering partial compaction within each subtask.
        // Note: Only force new group if current group already has content and is valid.
        bool would_exceed_segments = false;
        if (!is_pk_table && config.max_segments_per_subtask > 0 && !current_group.empty()) {
            would_exceed_segments = (current_segments + rowset_segments > config.max_segments_per_subtask);
            if (would_exceed_segments && current_group_valid && can_create_more_groups && has_enough_remaining) {
                should_start_new_group = true;
            }
        }

        if (should_start_new_group) {
            std::vector<uint32_t> group_ids;
            for (const auto& r : current_group) {
                group_ids.push_back(r->id());
            }
            std::string reason =
                    has_adjacency_gap ? " (adjacency gap)" : would_exceed_segments ? " (segment limit)" : "";
            VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " group " << valid_groups.size() << ": "
                    << current_group.size() << " rowsets, " << current_bytes << " bytes, " << current_segments
                    << " segments, ids=[" << JoinInts(group_ids, ",") << "]" << reason;

            valid_groups.push_back(std::move(current_group));
            current_group.clear();
            current_bytes = 0;
            current_segments = 0;
        }

        current_group.push_back(std::move(rowset));
        current_bytes += rowset_bytes;
        current_segments += rowset_segments;
        last_rowset_index = current_rowset_index;
    }

    // Add the last group if not empty
    if (!current_group.empty()) {
        std::vector<uint32_t> group_ids;
        for (const auto& r : current_group) {
            group_ids.push_back(r->id());
        }
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " group " << valid_groups.size() << ": "
                << current_group.size() << " rowsets, " << current_bytes << " bytes, " << current_segments
                << " segments, ids=[" << JoinInts(group_ids, ",") << "]";
        valid_groups.push_back(std::move(current_group));
    }

    if (skipped_rowsets > 0) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " skipped " << skipped_rowsets << " rowsets ("
                << skipped_bytes << " bytes) for later compaction due to capacity limit";
    }

    return valid_groups;
}

// Filter out invalid groups that cannot be compacted.
// Single-rowset groups are only valid if they are overlapped with multiple segments.
std::vector<std::vector<RowsetPtr>> TabletParallelCompactionManager::_filter_invalid_groups(
        int64_t tablet_id, std::vector<std::vector<RowsetPtr>> groups) {
    std::vector<std::vector<RowsetPtr>> filtered_groups;
    std::vector<uint32_t> discarded_rowset_ids;

    for (auto& group : groups) {
        if (group.size() >= 2) {
            filtered_groups.push_back(std::move(group));
        } else if (group.size() == 1) {
            const auto& rowset = group[0];
            const auto& meta = rowset->metadata();
            // An overlapped rowset with multiple segments can be compacted to merge its segments
            bool can_compact_alone = meta.overlapped() && meta.segments_size() >= 2;
            if (can_compact_alone) {
                filtered_groups.push_back(std::move(group));
                VLOG(1) << "Parallel compaction: tablet=" << tablet_id
                        << " keeping single overlapped rowset (id=" << rowset->id()
                        << ", segments=" << meta.segments_size() << ") as valid group";
            } else {
                discarded_rowset_ids.push_back(rowset->id());
            }
        }
    }

    if (!discarded_rowset_ids.empty()) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " discarded " << discarded_rowset_ids.size()
                << " single non-overlapped rowset groups (ids=[" << JoinInts(discarded_rowset_ids, ",")
                << "]) due to adjacency gaps, will be processed in next compaction cycle";
    }

    return filtered_groups;
}

TabletParallelCompactionManager::TabletParallelCompactionManager(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}

TabletParallelCompactionManager::~TabletParallelCompactionManager() = default;

StatusOr<std::vector<RowsetPtr>> TabletParallelCompactionManager::pick_rowsets_for_compaction(
        int64_t tablet_id, int64_t txn_id, int64_t version, bool force_base_compaction) {
    // Get tablet metadata
    ASSIGN_OR_RETURN(auto tablet, _tablet_mgr->get_tablet(tablet_id, version));
    const auto& metadata = tablet.metadata();
    if (!metadata) {
        return Status::NotFound(strings::Substitute("Tablet metadata not found: tablet_id=$0, txn_id=$1, version=$2",
                                                    tablet_id, txn_id, version));
    }

    // Log metadata details for debugging duplicate compaction issues
    std::vector<uint32_t> first_rowset_ids;
    int count = std::min(10, metadata->rowsets_size());
    for (int i = 0; i < count; i++) {
        first_rowset_ids.push_back(metadata->rowsets(i).id());
    }
    VLOG(1) << "Parallel compaction pick_rowsets: tablet=" << tablet_id << " version=" << version
            << " metadata_rowsets_size=" << metadata->rowsets_size() << " next_rowset_id=" << metadata->next_rowset_id()
            << " first_10_rowset_ids=[" << JoinInts(first_rowset_ids, ",") << "]";

    // Get all rowsets to compact using standard pick_rowsets()
    ASSIGN_OR_RETURN(auto policy, CompactionPolicy::create(_tablet_mgr, metadata, force_base_compaction));
    auto all_rowsets_or = policy->pick_rowsets();
    if (!all_rowsets_or.ok() || all_rowsets_or.value().empty()) {
        return Status::NotFound(strings::Substitute(
                "No rowsets to compact: tablet_id=$0, txn_id=$1, version=$2, force_base_compaction=$3, "
                "pick_rowsets_status=$4",
                tablet_id, txn_id, version, force_base_compaction,
                all_rowsets_or.ok() ? "OK(empty)" : all_rowsets_or.status().to_string()));
    }

    return std::move(all_rowsets_or.value());
}

std::vector<std::vector<RowsetPtr>> TabletParallelCompactionManager::split_rowsets_into_groups(
        int64_t tablet_id, std::vector<RowsetPtr> all_rowsets, int32_t max_parallel, int64_t max_bytes,
        bool is_pk_table) {
    // Step 1: Sort rowsets by their index in metadata to ensure adjacency when grouped.
    // This is critical because pick_rowsets() may return rowsets sorted by compaction score,
    // not by their position in metadata.
    std::sort(all_rowsets.begin(), all_rowsets.end(),
              [](const RowsetPtr& a, const RowsetPtr& b) { return a->index() < b->index(); });

    // Step 2: Filter out large non-overlapped rowsets that don't need compaction.
    all_rowsets = _filter_compactable_rowsets(tablet_id, std::move(all_rowsets));
    if (all_rowsets.empty()) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id
                << " all rowsets are large non-overlapped, no compaction needed";
        return {};
    }

    // Step 3: Calculate statistics about the rowsets.
    RowsetStats stats = _calculate_rowset_stats(all_rowsets);

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " total_rowsets=" << all_rowsets.size()
            << " total_segments=" << stats.total_segments << " total_bytes=" << stats.total_bytes
            << " max_bytes_per_subtask=" << max_bytes << " has_delete_predicate=" << stats.has_delete_predicate;

    // Step 4: Check early return conditions for falling back to normal compaction.
    // Conditions:
    // 1. Data size is small enough to fit in one subtask - no benefit from parallelism
    // 2. max_parallel is 1 (parallelism disabled)
    // 3. Has delete predicate (must be applied atomically)
    // 4. Not enough segments for meaningful parallel compaction (need at least 4 segments)
    constexpr int64_t kMinSegmentsForParallel = 4; // 2 subtasks * 2 segments each
    bool not_enough_segments = stats.total_segments < kMinSegmentsForParallel;

    if (stats.total_bytes <= max_bytes || max_parallel <= 1 || stats.has_delete_predicate || not_enough_segments) {
        std::vector<uint32_t> group_ids;
        for (const auto& r : all_rowsets) {
            group_ids.push_back(r->id());
        }
        std::string reason = stats.has_delete_predicate
                                     ? "has_delete_predicate"
                                     : (max_parallel <= 1)
                                               ? "max_parallel<=1"
                                               : not_enough_segments ? "not_enough_segments" : "data_size_small";
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " fallback to normal compaction (" << reason
                << "): " << all_rowsets.size() << " rowsets, " << stats.total_segments << " segments, "
                << stats.total_bytes << " bytes, ids=[" << JoinInts(group_ids, ",") << "]";
        return {};
    }

    // Step 5: Calculate grouping configuration (number of subtasks, target bytes, etc.)
    GroupingConfig config = _calculate_grouping_config(tablet_id, all_rowsets, stats, max_parallel, max_bytes);

    // Step 6: Group rowsets into subtasks.
    std::vector<std::vector<RowsetPtr>> groups =
            _group_rowsets_into_subtasks(tablet_id, std::move(all_rowsets), config, max_bytes, is_pk_table);

    // Step 7: Filter out invalid groups (single non-overlapped rowsets).
    std::vector<std::vector<RowsetPtr>> filtered_groups = _filter_invalid_groups(tablet_id, std::move(groups));

    // Log if only 1 valid group remains.
    if (filtered_groups.size() == 1) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id
                << " only 1 valid group after filtering, using single group mode";
    }

    return filtered_groups;
}

StatusOr<std::shared_ptr<TabletParallelCompactionState>>
TabletParallelCompactionManager::create_and_register_tablet_state(int64_t tablet_id, int64_t txn_id, int64_t version,
                                                                  int32_t max_parallel, int64_t max_bytes,
                                                                  std::shared_ptr<CompactionTaskCallback> callback,
                                                                  const ReleaseTokenFunc& release_token) {
    std::string state_key = make_state_key(tablet_id, txn_id);
    std::lock_guard<std::mutex> lock(_states_mutex);

    auto it = _tablet_states.find(state_key);
    if (it != _tablet_states.end()) {
        return Status::AlreadyExist(
                strings::Substitute("Parallel compaction already exists: tablet_id=$0, txn_id=$1, version=$2, "
                                    "existing_version=$3, existing_running_subtasks=$4, existing_completed_subtasks=$5",
                                    tablet_id, txn_id, version, it->second->version,
                                    it->second->running_subtasks.size(), it->second->completed_subtasks.size()));
    }

    auto state = std::make_shared<TabletParallelCompactionState>();
    state->tablet_id = tablet_id;
    state->txn_id = txn_id;
    state->version = version;
    state->max_parallel = max_parallel;
    state->max_bytes_per_subtask = max_bytes;
    state->callback = std::move(callback);
    state->release_token = release_token;
    _tablet_states[state_key] = state;

    return state;
}

StatusOr<int> TabletParallelCompactionManager::submit_subtasks(
        const std::shared_ptr<TabletParallelCompactionState>& state_ptr, std::vector<std::vector<RowsetPtr>> groups,
        bool force_base_compaction, ThreadPool* thread_pool, const AcquireTokenFunc& acquire_token,
        const ReleaseTokenFunc& release_token) {
    int64_t tablet_id = state_ptr->tablet_id;
    int64_t txn_id = state_ptr->txn_id;
    int64_t version = state_ptr->version;
    int32_t max_parallel = state_ptr->max_parallel;

    int subtasks_created = 0;
    int submitted_rowsets_count = 0;
    int64_t submitted_bytes = 0;

    for (size_t group_idx = 0; group_idx < groups.size(); group_idx++) {
        auto& rowsets = groups[group_idx];

        // Collect rowset IDs and calculate input bytes
        std::vector<uint32_t> rowset_ids;
        int64_t input_bytes = 0;
        for (const auto& rowset : rowsets) {
            rowset_ids.push_back(rowset->id());
            input_bytes += rowset->data_size();
        }
        submitted_rowsets_count += rowset_ids.size();
        submitted_bytes += input_bytes;

        int32_t subtask_id = static_cast<int32_t>(group_idx);

        // Mark rowsets as compacting and create subtask info
        {
            std::lock_guard<std::mutex> lock(state_ptr->mutex);
            mark_rowsets_compacting(state_ptr.get(), rowset_ids);

            SubtaskInfo info;
            info.subtask_id = subtask_id;
            info.input_rowset_ids = rowset_ids;
            info.input_bytes = input_bytes;
            info.start_time = ::time(nullptr);
            state_ptr->running_subtasks[subtask_id] = std::move(info);
            state_ptr->total_subtasks_created++;
        }

        _running_subtasks++;

        // Acquire limiter token before submitting
        if (!acquire_token()) {
            LOG(WARNING) << "Parallel compaction: failed to acquire limiter token for subtask " << subtask_id
                         << " tablet " << tablet_id << ", will skip remaining groups";
            // Failed to acquire token, revert state changes
            {
                std::lock_guard<std::mutex> lock(state_ptr->mutex);
                unmark_rowsets_compacting(state_ptr.get(), rowset_ids);
                state_ptr->running_subtasks.erase(subtask_id);
                state_ptr->total_subtasks_created--;
            }
            _running_subtasks--;

            if (subtasks_created == 0) {
                cleanup_tablet(tablet_id, txn_id);
                return Status::ResourceBusy(strings::Substitute(
                        "Failed to acquire limiter token for parallel compaction: tablet_id=$0, txn_id=$1, "
                        "version=$2, subtask_id=$3, total_groups=$4, max_parallel=$5",
                        tablet_id, txn_id, version, subtask_id, groups.size(), max_parallel));
            }
            break;
        }

        // Submit task to thread pool
        auto submit_st = thread_pool->submit_func([this, tablet_id, txn_id, subtask_id, rowsets = std::move(rowsets),
                                                   version, force_base_compaction, release_token]() mutable {
            execute_subtask(tablet_id, txn_id, subtask_id, std::move(rowsets), version, force_base_compaction,
                            release_token);
        });

        if (!submit_st.ok()) {
            LOG(WARNING) << "Parallel compaction: failed to submit subtask " << subtask_id << " for tablet "
                         << tablet_id << ": " << submit_st;
            // Failed to submit, revert state changes and release token
            {
                std::lock_guard<std::mutex> lock(state_ptr->mutex);
                unmark_rowsets_compacting(state_ptr.get(), rowset_ids);
                state_ptr->running_subtasks.erase(subtask_id);
                state_ptr->total_subtasks_created--;
            }
            _running_subtasks--;
            release_token(false);

            if (subtasks_created == 0) {
                cleanup_tablet(tablet_id, txn_id);
                return submit_st;
            }
            break;
        }

        subtasks_created++;

        VLOG(1) << "Parallel compaction: created subtask " << subtask_id << " for tablet " << tablet_id
                << ", txn_id=" << txn_id << ", rowsets=" << rowset_ids.size() << " (ids: " << JoinInts(rowset_ids, ",")
                << "), input_bytes=" << input_bytes;
    }

    if (subtasks_created == 0) {
        cleanup_tablet(tablet_id, txn_id);
        // Note: Cannot calculate total_rowsets/total_bytes here because groups elements may have been
        // moved into lambdas during the loop. This edge case should rarely happen since we return
        // immediately when subtask creation fails on the first group.
        return Status::NotFound(strings::Substitute(
                "Failed to create any subtask for parallel compaction: tablet_id=$0, txn_id=$1, version=$2, "
                "total_groups=$3, max_parallel=$4",
                tablet_id, txn_id, version, groups.size(), max_parallel));
    }

    VLOG(1) << "Parallel compaction: successfully created " << subtasks_created << " subtasks for tablet " << tablet_id
            << ", txn_id=" << txn_id << ", total_rowsets=" << submitted_rowsets_count
            << ", total_bytes=" << submitted_bytes;

    return subtasks_created;
}

// ================================================================================
// create_parallel_tasks - Create parallel compaction subtasks
// ================================================================================
//
// Purpose:
// --------
// Split a single tablet's compaction work into multiple subtasks that execute in parallel,
// accelerating the compaction process for large data volumes. Each subtask processes an
// independent group of rowsets, and results are merged at the end.
//
// Strategy:
// ---------
// 1. Rowset Selection: Use standard CompactionPolicy to select rowsets for compaction.
//
// 2. Grouping Strategy: Divide rowsets into groups based on max_bytes_per_subtask.
//    - Maximum number of groups is limited by max_parallel.
//    - If data volume exceeds max_parallel * max_bytes_per_subtask, excess rowsets are
//      skipped and left for the next compaction cycle.
//    - For small data volumes, falls back to single-task mode.
//    - Load Balancing: Calculates target_bytes_per_subtask = total_bytes / num_subtasks
//      to distribute data evenly across subtasks.
//      (Note: Rowsets are assigned in order and cannot be split, so balancing is best-effort)
//
// 3. Concurrent Execution: Each subtask executes compaction independently, generating
//    its own segments and rows_mapper files.
//
// 4. Result Merging: After all subtasks complete, merge TxnLogs (segments, statistics, etc.)
//    and execute SST compaction once.
//
// Partial Failure Handling:
// -------------------------
// - Subtasks execute independently; one failure does not interrupt others.
// - The tablet is marked as failed ONLY if ALL subtasks fail.
// - As long as at least one subtask succeeds, the tablet is considered (partially) successful
//   and can be published.
// - Merging strategy for successful subtasks:
//     * PK tables: Merge results from all successful subtasks (non-consecutive allowed)
//     * Non-PK tables: Merge only the longest consecutive sequence of successful subtasks
//       Example: [✓, ✗, ✓, ✓, ✓] → only use subtasks 2,3,4 (longest consecutive run of 3)
// - Rowsets from failed subtasks are not included in the merged TxnLog and will be
//   reprocessed in the next compaction cycle.
//
// ================================================================================
StatusOr<int> TabletParallelCompactionManager::create_parallel_tasks(
        int64_t tablet_id, int64_t txn_id, int64_t version, const TabletParallelConfig& config,
        std::shared_ptr<CompactionTaskCallback> callback, bool force_base_compaction, ThreadPool* thread_pool,
        const AcquireTokenFunc& acquire_token, const ReleaseTokenFunc& release_token) {
    // Validate configuration
    // max_parallel comes from table property (via FE)
    // max_bytes comes from BE config if FE passes 0
    int32_t max_parallel = config.max_parallel_per_tablet();
    int64_t max_bytes = config.max_bytes_per_subtask();

    // If FE doesn't specify max_bytes (passes 0), use BE config
    if (max_bytes <= 0) {
        max_bytes = starrocks::config::lake_compaction_max_bytes_per_subtask;
    }

    if (max_parallel <= 0 || max_bytes <= 0) {
        return Status::InvalidArgument(
                strings::Substitute("Invalid parallel compaction configuration: tablet_id=$0, txn_id=$1, version=$2, "
                                    "max_parallel=$3, max_bytes=$4, force_base_compaction=$5",
                                    tablet_id, txn_id, version, max_parallel, max_bytes, force_base_compaction));
    }

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " txn=" << txn_id << " version=" << version
            << " max_parallel=" << max_parallel << " max_bytes=" << max_bytes;

    // Get tablet metadata to check if it's a PK table
    ASSIGN_OR_RETURN(auto tablet, _tablet_mgr->get_tablet(tablet_id, version));
    const auto& metadata = tablet.metadata();
    bool is_pk_table = metadata->schema().keys_type() == PRIMARY_KEYS;

    // Parallel compaction only supports:
    // 1. PK tables with enable_pk_index_parallel_execution enabled
    // 2. Non-PK tables with size-tiered compaction strategy (no cumulative_point)
    // For PK tables, parallel compaction requires enable_pk_index_parallel_execution because
    // mapper files need to be stored on remote storage for multi-node access.
    // For non-PK tables with default (base+cumulative) strategy, the cumulative_point
    // calculation in parallel compaction is complex and error-prone, so we fallback
    // to normal compaction.
    if (is_pk_table && !config::enable_pk_index_parallel_execution) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " txn=" << txn_id
                << " fallback to normal compaction because enable_pk_index_parallel_execution is disabled";
        return 0;
    }
    if (!is_pk_table && !config::enable_size_tiered_compaction_strategy) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " txn=" << txn_id
                << " fallback to normal compaction because non-PK table with default compaction strategy";
        return 0;
    }

    // Step 1: Pick rowsets for compaction
    ASSIGN_OR_RETURN(auto all_rowsets, pick_rowsets_for_compaction(tablet_id, txn_id, version, force_base_compaction));
    size_t total_rowsets_count = all_rowsets.size();

    // Use the unified grouping algorithm that supports both large rowset splitting
    // and small rowset grouping. _split_large_rowset and execute_subtask_segment_range
    // are segment-based and do not depend on primary index, so this works for all table
    // types (PK, duplicate key, unique key, aggregate key).
    // Step 2: Create subtask groups (handles both large rowset split and small rowset grouping)
    auto subtask_groups = _create_subtask_groups(tablet_id, std::move(all_rowsets), max_parallel, max_bytes);

    if (subtask_groups.empty()) {
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " txn=" << txn_id
                << " fallback to normal compaction, total_rowsets=" << total_rowsets_count;
        return 0;
    }

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " created " << subtask_groups.size()
            << " subtask groups from " << total_rowsets_count << " rowsets";

    // Step 3: Create and register tablet state
    ASSIGN_OR_RETURN(auto state_ptr, create_and_register_tablet_state(tablet_id, txn_id, version, max_parallel,
                                                                      max_bytes, std::move(callback), release_token));

    // Step 4: Submit subtasks
    return submit_subtasks_from_groups(std::move(state_ptr), std::move(subtask_groups), force_base_compaction,
                                       thread_pool, acquire_token, release_token);
}

std::shared_ptr<TabletParallelCompactionState> TabletParallelCompactionManager::get_tablet_state(int64_t tablet_id,
                                                                                                 int64_t txn_id) {
    std::string state_key = make_state_key(tablet_id, txn_id);
    std::lock_guard<std::mutex> lock(_states_mutex);
    auto it = _tablet_states.find(state_key);
    if (it == _tablet_states.end()) {
        return nullptr;
    }
    return it->second; // Return shared_ptr copy to keep state alive
}

void TabletParallelCompactionManager::on_subtask_complete(int64_t tablet_id, int64_t txn_id, int32_t subtask_id,
                                                          std::unique_ptr<CompactionTaskContext> context) {
    std::string state_key = make_state_key(tablet_id, txn_id);

    // Get a shared_ptr copy to prevent use-after-free if cleanup_tablet is called concurrently.
    // The shared_ptr keeps the state alive even if it's removed from _tablet_states map.
    std::shared_ptr<TabletParallelCompactionState> state;
    {
        std::lock_guard<std::mutex> lock(_states_mutex);
        auto it = _tablet_states.find(state_key);
        if (it == _tablet_states.end()) {
            // State was cleaned up (e.g., due to timeout or cancellation from FE).
            // We must still decrement the counter since it was incremented in create_parallel_tasks.
            _running_subtasks--;
            LOG(WARNING) << "Tablet state not found for subtask completion, tablet=" << tablet_id
                         << ", txn_id=" << txn_id << ", subtask_id=" << subtask_id
                         << ". Decremented running_subtasks counter.";
            return;
        }
        state = it->second; // Copy shared_ptr to extend lifetime
    }

    std::shared_ptr<CompactionTaskCallback> callback;
    bool all_complete = false;

    {
        std::lock_guard<std::mutex> lock(state->mutex);

        auto it = state->running_subtasks.find(subtask_id);
        if (it == state->running_subtasks.end()) {
            // Subtask was already removed (should not happen in normal flow, but handle defensively).
            // We must still decrement the counter to prevent counter leakage.
            _running_subtasks--;
            LOG(WARNING) << "Subtask not found, tablet=" << tablet_id << ", txn_id=" << txn_id
                         << ", subtask_id=" << subtask_id << ". Decremented running_subtasks counter.";
            return;
        }

        // Unmark rowsets
        unmark_rowsets_compacting(state.get(), it->second.input_rowset_ids);

        // Move to completed
        state->completed_subtasks.push_back(std::move(context));
        state->running_subtasks.erase(it);

        _running_subtasks--;
        _completed_subtasks++;

        all_complete = state->is_complete();
        callback = state->callback;

        VLOG(1) << "Parallel compaction subtask completed, tablet=" << tablet_id << ", txn_id=" << txn_id
                << ", subtask_id=" << subtask_id << ", remaining=" << state->running_subtasks.size()
                << ", completed=" << state->completed_subtasks.size();
    }

    // If all subtasks are complete, notify the callback
    if (all_complete && callback) {
        // Build merged context
        // Note: skip_write_txnlog must be true so that merged txn_log is added to response
        auto merged_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, state->version,
                                                                      false /* force_base_compaction */,
                                                                      true /* skip_write_txnlog */, callback);

        // Copy table_id and partition_id from one of the completed subtask contexts.
        // These values are populated in TabletManager::compact() from shard info,
        // and are needed for downstream processes (e.g., metrics, catalog operations).
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            for (const auto& subtask_ctx : state->completed_subtasks) {
                if (subtask_ctx->table_id != 0) {
                    merged_context->table_id = subtask_ctx->table_id;
                }
                if (subtask_ctx->partition_id != 0) {
                    merged_context->partition_id = subtask_ctx->partition_id;
                }
                // All subtasks belong to the same tablet, so table_id and partition_id
                // should be the same. Break once we've found valid values.
                if (merged_context->table_id != 0 && merged_context->partition_id != 0) {
                    break;
                }
            }
        }

        // Merge TxnLogs
        auto merged_txn_log_or = get_merged_txn_log(tablet_id, txn_id);
        if (merged_txn_log_or.ok()) {
            merged_context->txn_log = std::make_unique<TxnLogPB>(std::move(merged_txn_log_or.value()));
        } else {
            merged_context->status = merged_txn_log_or.status();
        }

        // Set status based on subtask statuses
        // Support partial success: only fail if ALL subtasks failed
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            int successful_count = 0;
            int failed_count = 0;
            Status first_failure;

            for (const auto& subtask_ctx : state->completed_subtasks) {
                if (subtask_ctx->status.ok()) {
                    successful_count++;
                } else {
                    failed_count++;
                    if (first_failure.ok()) {
                        first_failure = subtask_ctx->status;
                    }
                }
                // Merge stats from all subtasks (including failed ones for diagnostic purposes)
                if (subtask_ctx->stats) {
                    *(merged_context->stats) = *(merged_context->stats) + *(subtask_ctx->stats);
                }
            }

            // Only mark as failed if ALL subtasks failed
            // If at least one subtask succeeded, the compaction is considered (partially) successful
            if (successful_count == 0 && failed_count > 0) {
                merged_context->status = first_failure;
                LOG(WARNING) << "Parallel compaction failed: all " << failed_count
                             << " subtasks failed for tablet=" << tablet_id << ", txn_id=" << txn_id
                             << ", first_error=" << first_failure;
            } else if (failed_count > 0) {
                // Partial success: some subtasks failed but at least one succeeded
                VLOG(1) << "Parallel compaction partial success: tablet=" << tablet_id << ", txn_id=" << txn_id
                        << ", successful=" << successful_count << ", failed=" << failed_count;
            }
        }

        // Mark as parallel merged context so that cleanup_tablet will be called
        // by CompactionScheduler::remove_states after RPC response is sent.
        // This ensures parallel compaction tasks remain visible in list_tasks
        // until all tablets complete, consistent with normal compaction behavior.
        merged_context->is_parallel_merged = true;

        // Set subtask_count to the actual number of subtasks
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            merged_context->subtask_count = static_cast<int32_t>(state->completed_subtasks.size());
        }

        callback->finish_task(std::move(merged_context));
        // Note: Do NOT call cleanup_tablet here. The cleanup will be done by
        // CompactionScheduler::remove_states when RPC response is sent.
    }
}

bool TabletParallelCompactionManager::is_tablet_complete(int64_t tablet_id, int64_t txn_id) {
    auto state = get_tablet_state(tablet_id, txn_id);
    if (state == nullptr) {
        return true;
    }
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->is_complete();
}

void TabletParallelCompactionManager::cleanup_tablet(int64_t tablet_id, int64_t txn_id) {
    std::string state_key = make_state_key(tablet_id, txn_id);

    {
        std::lock_guard<std::mutex> lock(_states_mutex);
        auto it = _tablet_states.find(state_key);
        if (it != _tablet_states.end()) {
            // Use nested scope to ensure state_lock is released before erase.
            // Otherwise, erase() destroys the mutex while state_lock still holds it,
            // causing use-after-free when state_lock's destructor tries to unlock.
            {
                std::lock_guard<std::mutex> state_lock(it->second->mutex);
                // Just access to synchronize, no need to get subtask_count anymore
            }
            _tablet_states.erase(it);
        }
    }

    // NOTE: Do NOT delete rows mapper files here!
    // These files are needed by light_publish_primary_compaction which happens in a
    // separate publish RPC after compaction completes. Each subtask's rows mapper file
    // is cleaned up by RowsMapperIterator in its destructor during _light_publish_subtask().

    VLOG(1) << "Cleaned up parallel compaction state for tablet " << tablet_id << ", txn_id=" << txn_id;
}

StatusOr<TxnLogPB> TabletParallelCompactionManager::get_merged_txn_log(int64_t tablet_id, int64_t txn_id) {
    auto state = get_tablet_state(tablet_id, txn_id);
    if (state == nullptr) {
        return Status::NotFound(strings::Substitute(
                "Tablet state not found for get_merged_txn_log: tablet_id=$0, txn_id=$1", tablet_id, txn_id));
    }

    TxnLogPB merged_log;
    merged_log.set_tablet_id(tablet_id);
    merged_log.set_txn_id(txn_id);

    // Use the new OpParallelCompaction format
    auto* op_parallel = merged_log.mutable_op_parallel_compaction();

    // Variables to be used outside the lock
    int64_t version = 0;
    std::vector<int32_t> success_subtask_ids;

    struct LcrmMergeTask {
        int merged_compaction_idx;
        std::vector<FileMetaPB> lcrm_files;
        std::vector<int64_t> num_rows_per_subtask;
    };
    std::vector<LcrmMergeTask> lcrm_merge_tasks;

    {
        std::lock_guard<std::mutex> lock(state->mutex);

        // Cache version for use outside the lock
        version = state->version;

        // Sort completed_subtasks by subtask_id to ensure consistent ordering
        // This is critical because rows_mapper files are merged by subtask_id order,
        // so segments must also be merged in the same order.
        std::sort(state->completed_subtasks.begin(), state->completed_subtasks.end(),
                  [](const std::unique_ptr<CompactionTaskContext>& a, const std::unique_ptr<CompactionTaskContext>& b) {
                      return a->subtask_id < b->subtask_id;
                  });

        // Build a map from subtask_id to status for quick lookup
        std::unordered_map<int32_t, bool> subtask_success_map;
        for (const auto& ctx : state->completed_subtasks) {
            subtask_success_map[ctx->subtask_id] = ctx->status.ok();
        }

        // For large rowset split groups, check if ALL subtasks in the group succeeded
        // AND the actual count matches the expected count.
        // If any subtask in a group failed, or if the group is incomplete (some subtasks
        // were not created due to acquire_token() failure), the entire group is considered failed.
        // This ensures data consistency - we either replace the entire large rowset or keep it unchanged.
        std::unordered_set<uint32_t> failed_large_rowset_ids;
        for (const auto& [large_rowset_id, subtask_ids] : state->large_rowset_split_groups) {
            // Check if the split is complete by comparing actual vs expected subtask count
            auto expected_it = state->expected_large_rowset_split_counts.find(large_rowset_id);
            int32_t expected_count = (expected_it != state->expected_large_rowset_split_counts.end())
                                             ? expected_it->second
                                             : static_cast<int32_t>(subtask_ids.size());
            bool is_complete = (static_cast<int32_t>(subtask_ids.size()) == expected_count);

            if (!is_complete) {
                failed_large_rowset_ids.insert(large_rowset_id);
                LOG(WARNING) << "Large rowset split group incomplete: tablet=" << tablet_id << ", txn_id=" << txn_id
                             << ", large_rowset_id=" << large_rowset_id << ", actual_subtasks=" << subtask_ids.size()
                             << ", expected_subtasks=" << expected_count << ", created_subtask_ids=["
                             << JoinInts(subtask_ids, ",") << "]"
                             << ". All subtasks in this group will be skipped to prevent data loss.";
                continue;
            }

            // Check if all subtasks in the group succeeded
            bool all_success = true;
            for (int32_t sid : subtask_ids) {
                auto it = subtask_success_map.find(sid);
                if (it == subtask_success_map.end() || !it->second) {
                    all_success = false;
                    break;
                }
            }
            if (!all_success) {
                failed_large_rowset_ids.insert(large_rowset_id);
                LOG(WARNING) << "Large rowset split group failed: tablet=" << tablet_id << ", txn_id=" << txn_id
                             << ", large_rowset_id=" << large_rowset_id << ", subtask_ids=["
                             << JoinInts(subtask_ids, ",") << "]"
                             << ". All subtasks in this group will be skipped.";
            }
        }

        // Build a map from subtask_id to its large_rowset_id (if it's a LARGE_ROWSET_PART type)
        std::unordered_map<int32_t, uint32_t> subtask_to_large_rowset;
        for (const auto& [large_rowset_id, subtask_ids] : state->large_rowset_split_groups) {
            for (int32_t sid : subtask_ids) {
                subtask_to_large_rowset[sid] = large_rowset_id;
            }
        }

        // Collect successful subtask IDs, excluding subtasks from failed large rowset groups
        for (const auto& ctx : state->completed_subtasks) {
            if (!ctx->status.ok()) {
                LOG(WARNING) << "Parallel compaction: skipping failed subtask " << ctx->subtask_id << " for tablet "
                             << tablet_id << ", txn_id=" << txn_id << ", status=" << ctx->status;
                continue;
            }

            // Check if this subtask belongs to a failed large rowset group
            auto it = subtask_to_large_rowset.find(ctx->subtask_id);
            if (it != subtask_to_large_rowset.end()) {
                if (failed_large_rowset_ids.count(it->second) > 0) {
                    LOG(WARNING) << "Parallel compaction: skipping subtask " << ctx->subtask_id
                                 << " because its large rowset group " << it->second << " failed";
                    continue;
                }
            }

            success_subtask_ids.push_back(ctx->subtask_id);
        }

        // If no successful subtasks, return error
        if (success_subtask_ids.empty()) {
            return Status::InternalError(strings::Substitute(
                    "All subtasks failed for parallel compaction: tablet_id=$0, txn_id=$1, total_subtasks=$2",
                    tablet_id, txn_id, state->completed_subtasks.size()));
        }

        std::unordered_set<int32_t> success_subtask_id_set(success_subtask_ids.begin(), success_subtask_ids.end());

        // Build a set of successful large rowset IDs for merging
        std::unordered_set<uint32_t> successful_large_rowset_ids;
        for (const auto& [large_rowset_id, subtask_ids] : state->large_rowset_split_groups) {
            if (failed_large_rowset_ids.count(large_rowset_id) == 0) {
                successful_large_rowset_ids.insert(large_rowset_id);
            }
        }

        // Track which subtasks have been processed (for large rowset split merging)
        std::unordered_set<int32_t> processed_subtask_ids;

        // Process large rowset split groups: merge all subtasks into a single OpCompaction
        for (const auto& [large_rowset_id, subtask_ids] : state->large_rowset_split_groups) {
            if (failed_large_rowset_ids.count(large_rowset_id) > 0) {
                continue; // Skip failed groups
            }

            // Create merged OpCompaction for this large rowset split
            int merged_compaction_idx = op_parallel->subtask_compactions_size();
            auto* merged_compaction = op_parallel->add_subtask_compactions();

            // Use the first subtask's input_rowsets (all subtasks share the same input)
            bool first_subtask = true;
            int64_t total_num_rows = 0;
            int64_t total_data_size = 0;
            LcrmMergeTask lcrm_task;
            lcrm_task.merged_compaction_idx = merged_compaction_idx;

            // Sort subtask_ids to ensure consistent segment ordering
            std::vector<int32_t> sorted_subtask_ids(subtask_ids.begin(), subtask_ids.end());
            std::sort(sorted_subtask_ids.begin(), sorted_subtask_ids.end());

            for (int32_t sid : sorted_subtask_ids) {
                // Find the context for this subtask
                const CompactionTaskContext* ctx = nullptr;
                for (const auto& c : state->completed_subtasks) {
                    if (c->subtask_id == sid) {
                        ctx = c.get();
                        break;
                    }
                }
                if (ctx == nullptr || ctx->txn_log == nullptr || !ctx->txn_log->has_op_compaction()) {
                    continue;
                }

                const auto& subtask_op = ctx->txn_log->op_compaction();

                if (first_subtask) {
                    // Copy input_rowsets from first subtask
                    for (int i = 0; i < subtask_op.input_rowsets_size(); i++) {
                        merged_compaction->add_input_rowsets(subtask_op.input_rowsets(i));
                    }
                    // Set subtask_id to the first subtask's id for tracking
                    merged_compaction->set_subtask_id(sid);
                    // Copy compact_version from the first subtask for conflict detection
                    if (subtask_op.has_compact_version()) {
                        merged_compaction->set_compact_version(subtask_op.compact_version());
                    }
                    first_subtask = false;
                }

                // Merge output_rowset segments
                if (subtask_op.has_output_rowset()) {
                    const auto& output = subtask_op.output_rowset();
                    auto* merged_output = merged_compaction->mutable_output_rowset();

                    // Add segments
                    for (int i = 0; i < output.segments_size(); i++) {
                        merged_output->add_segments(output.segments(i));
                    }
                    // Add segment_size
                    for (int i = 0; i < output.segment_size_size(); i++) {
                        merged_output->add_segment_size(output.segment_size(i));
                    }
                    // Add segment_encryption_metas
                    for (int i = 0; i < output.segment_encryption_metas_size(); i++) {
                        merged_output->add_segment_encryption_metas(output.segment_encryption_metas(i));
                    }
                    // Add segment_metas
                    for (int i = 0; i < output.segment_metas_size(); i++) {
                        merged_output->add_segment_metas()->CopyFrom(output.segment_metas(i));
                    }

                    total_num_rows += output.num_rows();
                    total_data_size += output.data_size();
                }

                // Merge ssts and sst_ranges (they are generated together in compaction)
                for (int i = 0; i < subtask_op.ssts_size(); i++) {
                    merged_compaction->add_ssts()->CopyFrom(subtask_op.ssts(i));
                }
                for (int i = 0; i < subtask_op.sst_ranges_size(); i++) {
                    merged_compaction->add_sst_ranges()->CopyFrom(subtask_op.sst_ranges(i));
                }

                // Collect subtask LCRM files for merging into a single LCRM
                if (subtask_op.has_lcrm_file()) {
                    lcrm_task.lcrm_files.push_back(subtask_op.lcrm_file());
                    lcrm_task.num_rows_per_subtask.push_back(
                            subtask_op.has_output_rowset() ? subtask_op.output_rowset().num_rows() : 0);
                    op_parallel->add_orphan_lcrm_files()->CopyFrom(subtask_op.lcrm_file());
                }

                // Mark this subtask as processed
                processed_subtask_ids.insert(sid);
            }

            // Collect LCRM merge task if subtask LCRMs are available
            if (!lcrm_task.lcrm_files.empty()) {
                lcrm_merge_tasks.push_back(std::move(lcrm_task));
            }

            // If no valid subtask was processed, remove the empty merged_compaction
            // to avoid undefined behavior when dereferencing empty input_rowsets
            if (first_subtask) {
                op_parallel->mutable_subtask_compactions()->RemoveLast();
                if (!lcrm_merge_tasks.empty() &&
                    lcrm_merge_tasks.back().merged_compaction_idx == merged_compaction_idx) {
                    lcrm_merge_tasks.pop_back();
                }
                LOG(WARNING) << "Large rowset split group has no valid subtasks, removing empty compaction"
                             << ", tablet=" << tablet_id << ", large_rowset_id=" << large_rowset_id;
                continue;
            }

            // Set merged output rowset properties
            if (merged_compaction->has_output_rowset()) {
                auto* merged_output = merged_compaction->mutable_output_rowset();
                merged_output->set_num_rows(total_num_rows);
                merged_output->set_data_size(total_data_size);
                merged_output->set_overlapped(true);
                merged_output->set_next_compaction_offset(merged_output->segments_size());
            }

            // Log segment file names for debugging data consistency
            std::stringstream seg_names;
            const auto& merged_output_rowset = merged_compaction->output_rowset();
            for (int i = 0; i < merged_output_rowset.segments_size(); i++) {
                if (i > 0) seg_names << ",";
                seg_names << merged_output_rowset.segments(i);
            }
            VLOG(1) << "Merged large rowset split result: tablet=" << tablet_id << ", txn_id=" << txn_id
                    << ", large_rowset_id=" << large_rowset_id << ", subtask_count=" << sorted_subtask_ids.size()
                    << ", total_segments=" << merged_output_rowset.segments_size() << ", total_rows=" << total_num_rows
                    << ", total_data_size=" << total_data_size << ", merged_ssts=" << merged_compaction->ssts_size()
                    << ", merged_sst_ranges=" << merged_compaction->sst_ranges_size()
                    << ", compact_version=" << merged_compaction->compact_version() << ", input_rowsets=["
                    << JoinInts(std::vector<uint32_t>(merged_compaction->input_rowsets().begin(),
                                                      merged_compaction->input_rowsets().end()),
                                ",")
                    << "]"
                    << ", orphan_lcrm_count=" << op_parallel->orphan_lcrm_files_size() << ", segment_files=["
                    << seg_names.str() << "]";
        }

        // Process normal subtasks (not part of large rowset split)
        for (const auto& ctx : state->completed_subtasks) {
            if (success_subtask_id_set.count(ctx->subtask_id) == 0) {
                continue;
            }
            // Skip if already processed as part of large rowset split
            if (processed_subtask_ids.count(ctx->subtask_id) > 0) {
                continue;
            }
            if (ctx->txn_log == nullptr || !ctx->txn_log->has_op_compaction()) {
                continue;
            }

            // Copy the entire OpCompaction from this normal subtask
            auto* subtask_compaction = op_parallel->add_subtask_compactions();
            subtask_compaction->CopyFrom(ctx->txn_log->op_compaction());
            subtask_compaction->set_subtask_id(ctx->subtask_id);
            // Clear segment_range fields for normal subtasks (not needed)
            subtask_compaction->clear_segment_range_start();
            subtask_compaction->clear_segment_range_end();
        }

        // Record success_subtask_ids for tracking
        for (int32_t id : success_subtask_ids) {
            op_parallel->add_success_subtask_ids(id);
        }

        size_t failed_count = state->completed_subtasks.size() - success_subtask_ids.size();
        if (failed_count > 0) {
            VLOG(1) << "Parallel compaction partial success: tablet=" << tablet_id << ", txn_id=" << txn_id
                    << ", successful=" << success_subtask_ids.size() << ", failed=" << failed_count
                    << ", success_subtask_ids=[" << JoinInts(success_subtask_ids, ",") << "]";
        }

        VLOG(1) << "Merged TxnLog for tablet " << tablet_id << ", txn_id=" << txn_id
                << ", total_subtasks=" << state->completed_subtasks.size()
                << ", successful_subtasks=" << success_subtask_ids.size()
                << ", subtask_compactions=" << op_parallel->subtask_compactions_size();
    }
    // Lock released here

    // Merge subtask LCRM files into a single LCRM for each large rowset split group.
    // This enables the light publish path (SST ingestion) instead of the full publish
    // path (try_replace), which is more robust and performant.
    for (auto& task : lcrm_merge_tasks) {
        auto st = _merge_subtask_lcrm_files(tablet_id, txn_id, task.lcrm_files, task.num_rows_per_subtask,
                                            op_parallel->mutable_subtask_compactions(task.merged_compaction_idx));
        if (!st.ok()) {
            LOG(WARNING) << "Failed to merge LCRM files for large rowset split, tablet=" << tablet_id
                         << ", txn_id=" << txn_id << ", status=" << st;
            return st;
        }
    }

    // Execute SST compaction once after all subtasks complete.
    // Store results in OpParallelCompaction (not in individual subtask OpCompactions).
    RETURN_IF_ERROR(execute_sst_compaction_for_parallel(tablet_id, version, op_parallel));

    return merged_log;
}

void TabletParallelCompactionManager::mark_rowsets_compacting(TabletParallelCompactionState* state,
                                                              const std::vector<uint32_t>& rowset_ids) {
    // Assumes state->mutex is already held
    // Use reference counting: increment count for each rowset_id.
    // For large rowset split, multiple subtasks share the same rowset_id,
    // so we increment the count each time instead of just inserting.
    for (uint32_t rid : rowset_ids) {
        state->compacting_rowsets[rid]++;
    }
}

void TabletParallelCompactionManager::unmark_rowsets_compacting(TabletParallelCompactionState* state,
                                                                const std::vector<uint32_t>& rowset_ids) {
    // Assumes state->mutex is already held
    // Use reference counting: decrement count for each rowset_id.
    // Only remove the entry when count reaches zero.
    // This ensures that for large rowset split, the rowset is only unmarked
    // when ALL subtasks using it have completed.
    for (uint32_t rid : rowset_ids) {
        auto it = state->compacting_rowsets.find(rid);
        if (it != state->compacting_rowsets.end()) {
            if (--it->second <= 0) {
                state->compacting_rowsets.erase(it);
            }
        }
    }
}

void TabletParallelCompactionManager::execute_subtask(int64_t tablet_id, int64_t txn_id, int32_t subtask_id,
                                                      std::vector<RowsetPtr> input_rowsets, int64_t version,
                                                      bool force_base_compaction,
                                                      const ReleaseTokenFunc& release_token) {
    VLOG(1) << "Executing parallel compaction subtask " << subtask_id << " for tablet " << tablet_id
            << ", txn_id=" << txn_id << ", rowsets=" << input_rowsets.size();

    // Get tablet state and callback
    auto state = get_tablet_state(tablet_id, txn_id);
    if (state == nullptr) {
        // State has been cleaned up (possibly due to timeout or cancellation).
        // We must still decrement the global counter to prevent counter leakage.
        // Note: Since state is gone, we cannot call on_subtask_complete or notify the callback.
        // This is acceptable because state cleanup implies the compaction request has been
        // abandoned (e.g., timeout from FE side).
        _running_subtasks--;
        // Release limiter token
        if (release_token) {
            release_token(false);
        }
        LOG(WARNING) << "Tablet state not found during subtask execution, tablet=" << tablet_id << ", txn_id=" << txn_id
                     << ", subtask_id=" << subtask_id << ". Decremented running_subtasks counter.";
        return;
    }

    // Create compaction context for this subtask
    // Note: skip_write_txnlog must be true for parallel compaction, so that txn_log is saved to context
    // and can be merged later in on_subtask_complete
    // Pass subtask_id so that rows_mapper files use subtask-specific filenames
    auto context = CompactionTaskContext::create_for_subtask(txn_id, tablet_id, version, force_base_compaction,
                                                             true /* skip_write_txnlog */, state->callback, subtask_id);

    auto start_time = ::time(nullptr);
    context->start_time.store(start_time, std::memory_order_relaxed);

    // Calculate in_queue_time_sec using the enqueue time from SubtaskInfo
    // Also store context pointer in SubtaskInfo for progress/status tracking in list_tasks()
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        auto it = state->running_subtasks.find(subtask_id);
        if (it != state->running_subtasks.end()) {
            int64_t enqueue_time = it->second.start_time;
            int64_t in_queue_time_sec = start_time > enqueue_time ? (start_time - enqueue_time) : 0;
            context->stats->in_queue_time_sec += in_queue_time_sec;
            // Store context pointer for real-time progress/status tracking
            it->second.context = context.get();
        }
    }

    // Create compaction task using pre-selected rowsets
    auto compaction_task_or = _tablet_mgr->compact(context.get(), std::move(input_rowsets));
    if (!compaction_task_or.ok()) {
        LOG(WARNING) << "Failed to create compaction task for tablet " << tablet_id << " subtask " << subtask_id << ": "
                     << compaction_task_or.status();
        context->status = compaction_task_or.status();
        on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
        // Release limiter token on early return
        if (release_token) {
            release_token(compaction_task_or.status().is_mem_limit_exceeded());
        }
        return;
    }

    auto compaction_task = compaction_task_or.value();

    // Increment runs counter to track that this subtask has actually started execution.
    // This is important for list_tasks() to correctly display the profile for completed subtasks,
    // as it checks "if (info.runs > 0 && ctx->stats)" before displaying the profile.
    context->runs.fetch_add(1, std::memory_order_relaxed);

    // Execute compaction
    // Note: We capture 'this', tablet_id, and txn_id instead of the raw 'state' pointer
    // because the state could be cleaned up by another thread calling cleanup_tablet()
    // while this subtask is executing. By capturing stable values and using get_tablet_state()
    // inside the lambda, we safely check if the state still exists before accessing it.
    auto cancel_func = [this, tablet_id, txn_id, subtask_id, version]() {
        // Check if tablet state still exists - if not, the compaction has been cancelled/timed out
        if (get_tablet_state(tablet_id, txn_id) == nullptr) {
            return Status::Cancelled(strings::Substitute(
                    "Tablet parallel compaction state has been cleaned up: tablet_id=$0, txn_id=$1, "
                    "version=$2, subtask_id=$3",
                    tablet_id, txn_id, version, subtask_id));
        }
        return Status::OK();
    };

    ThreadPool* flush_pool = nullptr;
    if (config::lake_enable_compaction_async_write) {
        flush_pool = StorageEngine::instance()->lake_memtable_flush_executor()->get_thread_pool();
    }

    auto exec_st = compaction_task->execute(cancel_func, flush_pool);

    auto finish_time = std::max<int64_t>(::time(nullptr), start_time);
    auto cost = finish_time - start_time;

    if (!exec_st.ok()) {
        LOG(WARNING) << "Compaction subtask " << subtask_id << " failed for tablet " << tablet_id << ": " << exec_st
                     << ", cost=" << cost << "s";
        context->status = exec_st;
    } else {
        VLOG(1) << "Compaction subtask " << subtask_id << " completed for tablet " << tablet_id << ", cost=" << cost
                << "s";
    }

    context->finish_time.store(finish_time, std::memory_order_release);

    // Check if memory limit was exceeded before moving context
    bool mem_limit_exceeded = exec_st.is_mem_limit_exceeded();

    // Notify completion
    on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));

    // Release limiter token after subtask completes
    if (release_token) {
        release_token(mem_limit_exceeded);
    }
}

Status TabletParallelCompactionManager::execute_sst_compaction_for_parallel(
        int64_t tablet_id, int64_t version, TxnLogPB_OpParallelCompaction* op_parallel) {
    // Get tablet metadata to check if SST compaction is applicable
    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    if (!tablet_or.ok()) {
        LOG(WARNING) << "Failed to get tablet for SST compaction, tablet=" << tablet_id << ", version=" << version
                     << ": " << tablet_or.status();
        return Status::OK(); // Don't fail the entire compaction for SST compaction failure
    }

    auto tablet = tablet_or.value();
    const auto& metadata = tablet.metadata();

    // Check if this is a primary key table with cloud native persistent index
    if (!metadata || metadata->schema().keys_type() != KeysType::PRIMARY_KEYS) {
        return Status::OK();
    }

    if (!metadata->enable_persistent_index() ||
        metadata->persistent_index_type() != PersistentIndexTypePB::CLOUD_NATIVE) {
        return Status::OK();
    }

    // Execute SST compaction
    VLOG(1) << "Executing unified SST compaction for parallel compaction, tablet=" << tablet_id
            << ", version=" << version;

    auto* update_mgr = _tablet_mgr->update_mgr();
    if (update_mgr == nullptr) {
        LOG(WARNING) << "UpdateManager is null, skip SST compaction for tablet " << tablet_id;
        return Status::OK();
    }

    // Use a temporary TxnLogPB to capture SST compaction results
    TxnLogPB temp_log;
    auto st = update_mgr->execute_index_major_compaction(metadata, &temp_log);
    if (!st.ok()) {
        LOG(WARNING) << "SST compaction failed for tablet " << tablet_id << ": " << st
                     << ". This will not fail the parallel compaction.";
        // Don't fail the entire parallel compaction for SST compaction failure
        // SST compaction can be retried in the next compaction cycle
        return Status::OK();
    }

    // Copy SST compaction results from temp_log.op_compaction() to op_parallel
    if (temp_log.has_op_compaction()) {
        const auto& temp_op = temp_log.op_compaction();

        // Copy input sstables
        for (const auto& input_sst : temp_op.input_sstables()) {
            op_parallel->add_input_sstables()->CopyFrom(input_sst);
        }

        // Copy output sstable (single output)
        if (temp_op.has_output_sstable()) {
            op_parallel->mutable_output_sstable()->CopyFrom(temp_op.output_sstable());
        }

        // Copy output sstables (multiple outputs from parallel SST compaction)
        for (const auto& output_sst : temp_op.output_sstables()) {
            op_parallel->add_output_sstables()->CopyFrom(output_sst);
        }

        // Log SST compaction results
        if (!temp_op.input_sstables().empty()) {
            size_t total_input_sst_size = 0;
            for (const auto& input_sst : temp_op.input_sstables()) {
                total_input_sst_size += input_sst.filesize();
            }
            VLOG(1) << "SST compaction completed for tablet " << tablet_id
                    << ", input_ssts=" << temp_op.input_sstables_size() << ", input_size=" << total_input_sst_size;
        }
    }

    return Status::OK();
}

void TabletParallelCompactionManager::list_tasks(std::vector<CompactionTaskInfo>* infos) {
    std::lock_guard<std::mutex> lock(_states_mutex);
    for (const auto& [state_key, state_ptr] : _tablet_states) {
        std::lock_guard<std::mutex> state_lock(state_ptr->mutex);

        // Add running subtasks
        for (const auto& [subtask_id, subtask_info] : state_ptr->running_subtasks) {
            auto& info = infos->emplace_back();
            info.txn_id = state_ptr->txn_id;
            info.tablet_id = state_ptr->tablet_id;
            info.version = state_ptr->version;
            info.skipped = false;
            info.runs = 1; // Parallel subtasks run once
            info.start_time = subtask_info.start_time;
            info.finish_time = 0; // Still running
            // Get real-time progress from context if available.
            // Note: We only read progress (which uses atomic operations) for running tasks.
            // We don't read status here because context->status may be modified concurrently
            // in execute_subtask() without lock protection, which would cause a data race.
            // For running tasks, status is always shown as OK (in-progress).
            // Final status is only available after task completion (in completed_subtasks).
            if (subtask_info.context != nullptr) {
                info.progress = subtask_info.context->progress.value();
            } else {
                info.progress = 0;
            }
            info.status = Status::OK();
            // Build profile with subtask-specific info
            info.profile =
                    fmt::format(R"({{"subtask_id":{},"input_rowsets":{},"input_bytes":{},"is_parallel_subtask":true}})",
                                subtask_id, subtask_info.input_rowset_ids.size(), subtask_info.input_bytes);
        }

        // Add completed subtasks that haven't been cleaned up yet
        for (const auto& ctx : state_ptr->completed_subtasks) {
            auto& info = infos->emplace_back();
            info.txn_id = ctx->txn_id;
            info.tablet_id = ctx->tablet_id;
            info.version = ctx->version;
            info.skipped = ctx->skipped.load(std::memory_order_relaxed);
            info.runs = ctx->runs.load(std::memory_order_relaxed);
            info.start_time = ctx->start_time.load(std::memory_order_relaxed);
            info.finish_time = ctx->finish_time.load(std::memory_order_acquire);
            info.progress = ctx->progress.value();
            if (info.runs > 0 && ctx->stats) {
                info.profile = ctx->stats->to_json_stats();
            }
            if (info.finish_time > 0) {
                info.status = ctx->status;
            }
        }
    }
}

// ================================================================================
// Large rowset split related functions (all table types)
// ================================================================================

bool TabletParallelCompactionManager::_is_large_rowset_for_split(const RowsetPtr& rowset,
                                                                 int64_t max_bytes_per_subtask) {
    int64_t data_size = rowset->data_size();
    int64_t min_size = 2 * config::lake_compaction_max_rowset_size;
    const auto& meta = rowset->metadata();

    // Skip rowsets where all segments have already been processed by a previous
    // split compaction. Re-splitting would just produce the same overlapped output
    // and cause an infinite compaction loop.
    if (meta.next_compaction_offset() >= static_cast<uint32_t>(meta.segments_size())) {
        return false;
    }

    // A rowset is considered "large" and should be split if:
    // 1. data_size >= 2 * lake_compaction_max_rowset_size (at least twice the max rowset size)
    // 2. data_size > max_bytes_per_subtask (larger than what a single subtask should handle)
    // 3. segments_size >= 4 (enough segments to split into at least 2 subtasks with 2 segments each)
    // 4. is_overlapped (non-overlapped rowsets don't need segment-level compaction)
    return data_size >= min_size && data_size > max_bytes_per_subtask && meta.segments_size() >= 4 &&
           rowset->is_overlapped();
}

std::vector<SubtaskGroup> TabletParallelCompactionManager::_split_large_rowset(const RowsetPtr& rowset,
                                                                               int64_t target_bytes_per_subtask,
                                                                               int32_t max_subtasks) {
    std::vector<SubtaskGroup> groups;

    const auto& meta = rowset->metadata();
    int32_t total_segments = meta.segments_size();
    int64_t total_data_size = rowset->data_size();

    if (total_segments < 2 || total_data_size <= 0) {
        return groups;
    }

    // Estimate average bytes per segment
    int64_t avg_bytes_per_segment = total_data_size / total_segments;

    // Calculate how many subtasks we would need based on target_bytes_per_subtask
    int32_t ideal_num_subtasks = 1;
    if (target_bytes_per_subtask > 0) {
        ideal_num_subtasks =
                static_cast<int32_t>((total_data_size + target_bytes_per_subtask - 1) / target_bytes_per_subtask);
    }

    // Limit to max_subtasks if specified (> 0)
    // When max_subtasks is specified and less than ideal_num_subtasks, we allow each subtask
    // to process more data than target_bytes_per_subtask to ensure completeness
    int32_t actual_num_subtasks = ideal_num_subtasks;
    if (max_subtasks > 0 && actual_num_subtasks > max_subtasks) {
        actual_num_subtasks = max_subtasks;
        VLOG(1) << "Large rowset " << rowset->id() << " capped from " << ideal_num_subtasks << " to "
                << actual_num_subtasks << " subtasks (exceeding size limit to ensure completeness)";
    }

    // Ensure at least 1 subtask
    actual_num_subtasks = std::max(1, actual_num_subtasks);

    // Calculate segments per subtask for even distribution
    int32_t segments_per_subtask = (total_segments + actual_num_subtasks - 1) / actual_num_subtasks;

    // Ensure we have at least 2 segments per subtask for meaningful compaction
    segments_per_subtask = std::max(2, segments_per_subtask);

    // Recalculate actual number of subtasks based on segments_per_subtask
    actual_num_subtasks = (total_segments + segments_per_subtask - 1) / segments_per_subtask;

    // Split segments into groups
    int32_t segment_start = 0;
    while (segment_start < total_segments) {
        int32_t segment_end = std::min(segment_start + segments_per_subtask, total_segments);

        // Ensure we have at least 2 segments in the group (except for the last group)
        if (segment_end - segment_start < 2 && segment_start > 0) {
            // Merge with previous group
            if (!groups.empty()) {
                groups.back().segment_end = total_segments;
                groups.back().total_bytes = total_data_size - (groups.back().segment_start * avg_bytes_per_segment);
            }
            break;
        }

        SubtaskGroup group;
        group.type = SubtaskType::LARGE_ROWSET_PART;
        group.large_rowset = rowset;
        group.large_rowset_id = rowset->id();
        group.segment_start = segment_start;
        group.segment_end = segment_end;
        group.total_bytes = (segment_end - segment_start) * avg_bytes_per_segment;

        groups.push_back(std::move(group));
        segment_start = segment_end;
    }

    VLOG(1) << "Split large rowset " << rowset->id() << " into " << groups.size() << " subtasks"
            << ", total_segments=" << total_segments << ", total_bytes=" << total_data_size
            << ", segments_per_subtask=" << segments_per_subtask << ", ideal_num_subtasks=" << ideal_num_subtasks
            << ", max_subtasks=" << max_subtasks;

    return groups;
}

std::vector<SubtaskGroup> TabletParallelCompactionManager::_group_small_rowsets(std::vector<RowsetPtr> rowsets,
                                                                                int64_t target_bytes_per_subtask) {
    std::vector<SubtaskGroup> groups;

    if (rowsets.empty()) {
        return groups;
    }

    SubtaskGroup current_group;
    current_group.type = SubtaskType::NORMAL;
    current_group.total_bytes = 0;

    for (auto& rowset : rowsets) {
        int64_t rowset_bytes = rowset->data_size();

        // If adding this rowset would exceed the target and current group has content,
        // start a new group
        if (!current_group.rowsets.empty() && current_group.total_bytes + rowset_bytes > target_bytes_per_subtask) {
            // Only add groups with at least 1 rowset that needs compaction
            if (!current_group.rowsets.empty()) {
                groups.push_back(std::move(current_group));
                current_group = SubtaskGroup();
                current_group.type = SubtaskType::NORMAL;
                current_group.total_bytes = 0;
            }
        }

        current_group.rowsets.push_back(std::move(rowset));
        current_group.total_bytes += rowset_bytes;
    }

    // Add the last group
    if (!current_group.rowsets.empty()) {
        groups.push_back(std::move(current_group));
    }

    return groups;
}

std::vector<SubtaskGroup> TabletParallelCompactionManager::_create_subtask_groups(int64_t tablet_id,
                                                                                  std::vector<RowsetPtr> rowsets,
                                                                                  int32_t max_parallel,
                                                                                  int64_t max_bytes_per_subtask) {
    // Perform validation checks similar to split_rowsets_into_groups
    RowsetStats stats = _calculate_rowset_stats(rowsets);

    // Check early return conditions for falling back to normal compaction.
    constexpr int64_t kMinSegmentsForParallel = 4; // 2 subtasks * 2 segments each
    bool not_enough_segments = stats.total_segments < kMinSegmentsForParallel;

    if (stats.total_bytes <= max_bytes_per_subtask || max_parallel <= 1 || stats.has_delete_predicate ||
        not_enough_segments) {
        std::string reason = stats.has_delete_predicate
                                     ? "has_delete_predicate"
                                     : (max_parallel <= 1)
                                               ? "max_parallel<=1"
                                               : not_enough_segments ? "not_enough_segments" : "data_size_small";
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " fallback to normal compaction (" << reason
                << "): " << rowsets.size() << " rowsets, " << stats.total_segments << " segments, " << stats.total_bytes
                << " bytes";
        return {};
    }

    std::vector<SubtaskGroup> all_groups;

    // Separate large rowsets (to be split) from small rowsets (to be grouped)
    std::vector<RowsetPtr> large_rowsets;
    std::vector<RowsetPtr> small_rowsets;

    for (auto& rowset : rowsets) {
        if (_is_large_rowset_for_split(rowset, max_bytes_per_subtask)) {
            large_rowsets.push_back(std::move(rowset));
        } else {
            small_rowsets.push_back(std::move(rowset));
        }
    }

    VLOG(1) << "Create subtask groups: tablet=" << tablet_id << ", large_rowsets=" << large_rowsets.size()
            << ", small_rowsets=" << small_rowsets.size() << ", max_parallel=" << max_parallel
            << ", max_bytes_per_subtask=" << max_bytes_per_subtask;

    // Track remaining parallel slots
    int32_t remaining_parallel = max_parallel;

    // Process large rowsets first, ensuring each large rowset's split is complete.
    // Strategy:
    // 1. If a large rowset can be split within remaining_parallel slots, split it normally
    // 2. If a large rowset needs more slots than remaining_parallel AND remaining_parallel >= 2,
    //    cap it to remaining_parallel (allowing it to exceed max_bytes_per_subtask to ensure completeness)
    // 3. If remaining_parallel < 2, skip this large rowset (splitting with only 1 slot is meaningless)
    // 4. If no slots remain, skip this large rowset entirely (it won't participate in this compaction)
    for (auto& large_rowset : large_rowsets) {
        if (remaining_parallel <= 0) {
            VLOG(1) << "Skipping large rowset " << large_rowset->id()
                    << " - no remaining parallel slots (max_parallel=" << max_parallel << ")";
            continue;
        }

        // Calculate how many subtasks this rowset would need based on target_bytes_per_subtask
        int64_t total_data_size = large_rowset->data_size();
        int32_t ideal_subtasks = 1;
        if (max_bytes_per_subtask > 0) {
            ideal_subtasks =
                    static_cast<int32_t>((total_data_size + max_bytes_per_subtask - 1) / max_bytes_per_subtask);
        }

        // If ideal_subtasks > remaining_parallel, we need to cap it.
        // But capping only makes sense if remaining_parallel >= 2 (splitting with 1 slot is meaningless)
        if (ideal_subtasks > remaining_parallel && remaining_parallel < 2) {
            VLOG(1) << "Skipping large rowset " << large_rowset->id() << " - needs " << ideal_subtasks
                    << " subtasks but only " << remaining_parallel << " slots remaining (need at least 2 to split)";
            continue;
        }

        // Cap to remaining_parallel if needed (this may exceed max_bytes_per_subtask per group)
        int32_t actual_max_subtasks = std::min(ideal_subtasks, remaining_parallel);

        auto split_groups = _split_large_rowset(large_rowset, max_bytes_per_subtask, actual_max_subtasks);

        if (!split_groups.empty()) {
            remaining_parallel -= static_cast<int32_t>(split_groups.size());
            for (auto& g : split_groups) {
                all_groups.push_back(std::move(g));
            }
        }
    }

    // Group small rowsets into the remaining parallel slots
    if (remaining_parallel > 0 && !small_rowsets.empty()) {
        auto small_groups = _group_small_rowsets(std::move(small_rowsets), max_bytes_per_subtask);
        for (auto& g : small_groups) {
            if (remaining_parallel <= 0) {
                VLOG(1) << "Skipping remaining small rowset groups - no remaining parallel slots";
                break;
            }
            // Only add groups that are valid for compaction (>= 2 rowsets or 1 overlapped rowset)
            if (g.rowsets.size() >= 2 || (g.rowsets.size() == 1 && g.rowsets[0]->is_overlapped() &&
                                          g.rowsets[0]->metadata().segments_size() >= 2)) {
                all_groups.push_back(std::move(g));
                remaining_parallel--;
            } else {
                VLOG(1) << "Skipping small group with " << g.rowsets.size() << " rowsets (not enough for compaction)";
            }
        }
    }

    VLOG(1) << "Created " << all_groups.size() << " subtask groups for tablet " << tablet_id
            << " (max_parallel=" << max_parallel << ", used=" << (max_parallel - remaining_parallel) << ")";

    return all_groups;
}

StatusOr<int> TabletParallelCompactionManager::submit_subtasks_from_groups(
        const std::shared_ptr<TabletParallelCompactionState>& state_ptr, std::vector<SubtaskGroup> groups,
        bool force_base_compaction, ThreadPool* thread_pool, const AcquireTokenFunc& acquire_token,
        const ReleaseTokenFunc& release_token) {
    int64_t tablet_id = state_ptr->tablet_id;
    int64_t txn_id = state_ptr->txn_id;
    int64_t version = state_ptr->version;
    int32_t max_parallel = state_ptr->max_parallel;

    if (groups.empty()) {
        cleanup_tablet(tablet_id, txn_id);
        return Status::NotFound(strings::Substitute("No groups to submit: tablet_id=$0, txn_id=$1", tablet_id, txn_id));
    }

    // Acquire all tokens upfront to ensure atomic submission of all subtasks.
    // This prevents partial large-rowset splits which would cause data loss:
    // if only some subtasks of a large rowset split are created, get_merged_txn_log()
    // would treat the partial split as complete and the first subtask would delete
    // the original rowset, dropping segments that never got a subtask.
    int32_t total_groups = static_cast<int32_t>(groups.size());
    int32_t tokens_acquired = 0;

    for (int32_t i = 0; i < total_groups; i++) {
        if (!acquire_token()) {
            LOG(WARNING) << "Parallel compaction: failed to acquire limiter token " << i << "/" << total_groups
                         << " for tablet " << tablet_id << ", txn_id=" << txn_id << ". Releasing " << tokens_acquired
                         << " acquired tokens.";
            // Release all acquired tokens
            for (int32_t j = 0; j < tokens_acquired; j++) {
                release_token(false);
            }
            cleanup_tablet(tablet_id, txn_id);
            return Status::ResourceBusy(strings::Substitute(
                    "Failed to acquire all limiter tokens: tablet_id=$0, txn_id=$1, acquired=$2, total=$3", tablet_id,
                    txn_id, tokens_acquired, total_groups));
        }
        tokens_acquired++;
    }

    VLOG(1) << "Parallel compaction: acquired all " << tokens_acquired << " tokens for tablet " << tablet_id
            << ", txn_id=" << txn_id;

    // Record expected split counts for each large rowset as a safety net.
    // Even though we've acquired all tokens, thread pool submission can still fail.
    // This allows get_merged_txn_log() to detect incomplete splits.
    {
        std::unordered_map<uint32_t, int32_t> expected_counts;
        for (const auto& group : groups) {
            if (group.type == SubtaskType::LARGE_ROWSET_PART) {
                expected_counts[group.large_rowset_id]++;
            }
        }
        if (!expected_counts.empty()) {
            std::lock_guard<std::mutex> lock(state_ptr->mutex);
            state_ptr->expected_large_rowset_split_counts = std::move(expected_counts);
        }
    }

    // Now create and submit all subtasks. Since we've acquired all tokens,
    // we won't have partial large-rowset splits due to token exhaustion.
    // Thread pool submission failures are still possible but rare.
    int subtasks_created = 0;
    int64_t submitted_bytes = 0;

    for (size_t group_idx = 0; group_idx < groups.size(); group_idx++) {
        auto& group = groups[group_idx];
        int32_t subtask_id = static_cast<int32_t>(group_idx);

        // Collect rowset IDs
        std::vector<uint32_t> rowset_ids;
        int64_t input_bytes = group.total_bytes;

        if (group.type == SubtaskType::NORMAL) {
            for (const auto& rowset : group.rowsets) {
                rowset_ids.push_back(rowset->id());
            }
        } else {
            // LARGE_ROWSET_PART: only one rowset
            rowset_ids.push_back(group.large_rowset_id);
        }
        submitted_bytes += input_bytes;

        // Mark rowsets as compacting and create subtask info
        {
            std::lock_guard<std::mutex> lock(state_ptr->mutex);
            mark_rowsets_compacting(state_ptr.get(), rowset_ids);

            SubtaskInfo info;
            info.subtask_id = subtask_id;
            info.input_rowset_ids = rowset_ids;
            info.input_bytes = input_bytes;
            info.start_time = ::time(nullptr);
            info.type = group.type;
            if (group.type == SubtaskType::LARGE_ROWSET_PART) {
                info.large_rowset_id = group.large_rowset_id;
                info.segment_start = group.segment_start;
                info.segment_end = group.segment_end;

                // Record in large_rowset_split_groups for tracking
                state_ptr->large_rowset_split_groups[group.large_rowset_id].push_back(subtask_id);
            }
            state_ptr->running_subtasks[subtask_id] = std::move(info);
            state_ptr->total_subtasks_created++;
        }

        _running_subtasks++;

        // Submit task to thread pool (token already acquired)
        Status submit_st;
        if (group.type == SubtaskType::NORMAL) {
            auto rowsets = std::move(group.rowsets);
            submit_st = thread_pool->submit_func([this, tablet_id, txn_id, subtask_id, rowsets = std::move(rowsets),
                                                  version, force_base_compaction, release_token]() mutable {
                execute_subtask(tablet_id, txn_id, subtask_id, std::move(rowsets), version, force_base_compaction,
                                release_token);
            });
        } else {
            // LARGE_ROWSET_PART: create a segment-range rowset
            auto large_rowset = group.large_rowset;
            auto large_rowset_id = group.large_rowset_id;
            auto segment_start = group.segment_start;
            auto segment_end = group.segment_end;
            submit_st = thread_pool->submit_func([this, tablet_id, txn_id, subtask_id, large_rowset, large_rowset_id,
                                                  segment_start, segment_end, version, force_base_compaction,
                                                  release_token]() mutable {
                execute_subtask_segment_range(tablet_id, txn_id, subtask_id, large_rowset, large_rowset_id,
                                              segment_start, segment_end, version, force_base_compaction,
                                              release_token);
            });
        }

        if (!submit_st.ok()) {
            LOG(WARNING) << "Parallel compaction: failed to submit subtask " << subtask_id << " for tablet "
                         << tablet_id << ": " << submit_st;
            // Revert state changes
            {
                std::lock_guard<std::mutex> lock(state_ptr->mutex);
                unmark_rowsets_compacting(state_ptr.get(), rowset_ids);
                state_ptr->running_subtasks.erase(subtask_id);
                state_ptr->total_subtasks_created--;
                if (group.type == SubtaskType::LARGE_ROWSET_PART) {
                    auto& split_ids = state_ptr->large_rowset_split_groups[group.large_rowset_id];
                    split_ids.erase(std::remove(split_ids.begin(), split_ids.end(), subtask_id), split_ids.end());
                }
            }
            _running_subtasks--;

            // Release token for this failed subtask
            release_token(false);

            // Release remaining tokens that haven't been used yet
            int32_t remaining_tokens = total_groups - static_cast<int32_t>(group_idx) - 1;
            for (int32_t j = 0; j < remaining_tokens; j++) {
                release_token(false);
            }

            if (subtasks_created == 0) {
                cleanup_tablet(tablet_id, txn_id);
                return submit_st;
            }
            // Note: If some subtasks were already submitted, we break and let them run.
            // This is a thread pool submission failure, not a token acquisition failure,
            // so partial execution is acceptable for NORMAL type groups.
            // For LARGE_ROWSET_PART groups, the incomplete split detection in
            // get_merged_txn_log will handle this case.
            break;
        }

        subtasks_created++;

        if (group.type == SubtaskType::NORMAL) {
            VLOG(1) << "Parallel compaction: created NORMAL subtask " << subtask_id << " for tablet " << tablet_id
                    << ", txn_id=" << txn_id << ", rowsets=" << rowset_ids.size() << ", input_bytes=" << input_bytes;
        } else {
            VLOG(1) << "Parallel compaction: created LARGE_ROWSET_PART subtask " << subtask_id << " for tablet "
                    << tablet_id << ", txn_id=" << txn_id << ", large_rowset_id=" << group.large_rowset_id
                    << ", segment_range=[" << group.segment_start << "," << group.segment_end << ")"
                    << ", input_bytes=" << input_bytes;
        }
    }

    if (subtasks_created == 0) {
        cleanup_tablet(tablet_id, txn_id);
        return Status::NotFound(strings::Substitute(
                "Failed to create any subtask: tablet_id=$0, txn_id=$1, total_groups=$2, max_parallel=$3", tablet_id,
                txn_id, groups.size(), max_parallel));
    }

    VLOG(1) << "Parallel compaction: successfully created " << subtasks_created << " subtasks for tablet " << tablet_id
            << ", txn_id=" << txn_id << ", total_bytes=" << submitted_bytes;

    return subtasks_created;
}

void TabletParallelCompactionManager::execute_subtask_segment_range(int64_t tablet_id, int64_t txn_id,
                                                                    int32_t subtask_id, const RowsetPtr& input_rowset,
                                                                    uint32_t large_rowset_id, int32_t segment_start,
                                                                    int32_t segment_end, int64_t version,
                                                                    bool force_base_compaction,
                                                                    const ReleaseTokenFunc& release_token) {
    VLOG(1) << "Executing parallel compaction segment-range subtask " << subtask_id << " for tablet " << tablet_id
            << ", txn_id=" << txn_id << ", large_rowset_id=" << large_rowset_id << ", segment_range=[" << segment_start
            << "," << segment_end << ")";

    // Get tablet state
    auto state = get_tablet_state(tablet_id, txn_id);
    if (state == nullptr) {
        _running_subtasks--;
        if (release_token) {
            release_token(false);
        }
        LOG(WARNING) << "Tablet state not found during segment-range subtask execution, tablet=" << tablet_id
                     << ", txn_id=" << txn_id << ", subtask_id=" << subtask_id;
        return;
    }

    // Create compaction context
    auto context = CompactionTaskContext::create_for_subtask(txn_id, tablet_id, version, force_base_compaction,
                                                             true /* skip_write_txnlog */, state->callback, subtask_id);

    auto start_time = ::time(nullptr);
    context->start_time.store(start_time, std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        auto it = state->running_subtasks.find(subtask_id);
        if (it != state->running_subtasks.end()) {
            int64_t enqueue_time = it->second.start_time;
            int64_t in_queue_time_sec = start_time > enqueue_time ? (start_time - enqueue_time) : 0;
            context->stats->in_queue_time_sec += in_queue_time_sec;
            it->second.context = context.get();
        }
    }

    // Get tablet metadata to create segment-range rowset
    auto tablet_or = _tablet_mgr->get_tablet(tablet_id, version);
    if (!tablet_or.ok()) {
        LOG(WARNING) << "Failed to get tablet for segment-range subtask " << subtask_id << ": " << tablet_or.status();
        context->status = tablet_or.status();
        on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
        if (release_token) {
            release_token(false);
        }
        return;
    }

    auto& tablet = tablet_or.value();
    const auto& metadata = tablet.metadata();

    // Find the rowset index in metadata
    int rowset_index = -1;
    for (int i = 0; i < metadata->rowsets_size(); i++) {
        if (metadata->rowsets(i).id() == large_rowset_id) {
            rowset_index = i;
            break;
        }
    }

    if (rowset_index < 0) {
        LOG(WARNING) << "Rowset " << large_rowset_id << " not found in metadata for tablet " << tablet_id;
        context->status = Status::NotFound(strings::Substitute("Rowset $0 not found in metadata", large_rowset_id));
        on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
        if (release_token) {
            release_token(false);
        }
        return;
    }

    // Create segment-range rowset
    auto segment_range_rowset =
            std::make_shared<Rowset>(_tablet_mgr, metadata, rowset_index, segment_start, segment_end);

    // Create compaction task using segment-range rowset
    std::vector<RowsetPtr> input_rowsets;
    input_rowsets.push_back(std::move(segment_range_rowset));

    auto compaction_task_or = _tablet_mgr->compact(context.get(), std::move(input_rowsets));
    if (!compaction_task_or.ok()) {
        LOG(WARNING) << "Failed to create compaction task for segment-range subtask " << subtask_id << ": "
                     << compaction_task_or.status();
        context->status = compaction_task_or.status();
        on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));
        if (release_token) {
            release_token(compaction_task_or.status().is_mem_limit_exceeded());
        }
        return;
    }

    auto compaction_task = compaction_task_or.value();
    context->runs.fetch_add(1, std::memory_order_relaxed);

    // Execute compaction
    auto cancel_func = [this, tablet_id, txn_id, subtask_id, version]() {
        if (get_tablet_state(tablet_id, txn_id) == nullptr) {
            return Status::Cancelled(strings::Substitute(
                    "Tablet parallel compaction state has been cleaned up: tablet_id=$0, txn_id=$1, "
                    "version=$2, subtask_id=$3",
                    tablet_id, txn_id, version, subtask_id));
        }
        return Status::OK();
    };

    ThreadPool* flush_pool = nullptr;
    if (config::lake_enable_compaction_async_write) {
        flush_pool = StorageEngine::instance()->lake_memtable_flush_executor()->get_thread_pool();
    }

    auto exec_st = compaction_task->execute(cancel_func, flush_pool);

    auto finish_time = std::max<int64_t>(::time(nullptr), start_time);
    auto cost = finish_time - start_time;

    if (!exec_st.ok()) {
        LOG(WARNING) << "Segment-range compaction subtask " << subtask_id << " failed for tablet " << tablet_id << ": "
                     << exec_st << ", cost=" << cost << "s";
        context->status = exec_st;
    } else {
        VLOG(1) << "Segment-range compaction subtask " << subtask_id << " completed for tablet " << tablet_id
                << ", cost=" << cost << "s";

        // For segment-range subtask, we need to set segment_range in OpCompaction
        if (context->txn_log != nullptr && context->txn_log->has_op_compaction()) {
            auto* op_compaction = context->txn_log->mutable_op_compaction();
            op_compaction->set_segment_range_start(segment_start);
            op_compaction->set_segment_range_end(segment_end);
        }
    }

    context->finish_time.store(finish_time, std::memory_order_release);

    bool mem_limit_exceeded = exec_st.is_mem_limit_exceeded();
    on_subtask_complete(tablet_id, txn_id, subtask_id, std::move(context));

    if (release_token) {
        release_token(mem_limit_exceeded);
    }
}

Status TabletParallelCompactionManager::_merge_subtask_lcrm_files(int64_t tablet_id, int64_t txn_id,
                                                                  const std::vector<FileMetaPB>& lcrm_files,
                                                                  const std::vector<int64_t>& num_rows_per_subtask,
                                                                  TxnLogPB_OpCompaction* merged_compaction) {
    if (lcrm_files.empty()) {
        return Status::OK();
    }
    DCHECK(lcrm_files.size() == num_rows_per_subtask.size());

    ASSIGN_OR_RETURN(auto output_path, new_lake_rows_mapper_filename(_tablet_mgr, tablet_id, txn_id));

    auto merge_impl = [&]() -> Status {
        RowsMapperBuilder builder(output_path);

        static constexpr size_t kBatchSize = 65536;
        int64_t total_rows = 0;

        for (size_t i = 0; i < lcrm_files.size(); i++) {
            const auto& lcrm_file = lcrm_files[i];
            int64_t num_rows = num_rows_per_subtask[i];
            if (num_rows <= 0) {
                continue;
            }

            RowsMapperIterator iter;
            FileInfo info;
            ASSIGN_OR_RETURN(info.path, lake_rows_mapper_filename(_tablet_mgr, tablet_id, lcrm_file.name()));
            if (lcrm_file.size() > 0) {
                info.size = lcrm_file.size();
            }
            RETURN_IF_ERROR(iter.open(info));

            int64_t remaining = num_rows;
            while (remaining > 0) {
                size_t fetch = std::min(remaining, (int64_t)kBatchSize);
                std::vector<uint64_t> batch;
                RETURN_IF_ERROR(iter.next_values(fetch, &batch));
                RETURN_IF_ERROR(builder.append(batch));
                remaining -= fetch;
            }
            RETURN_IF_ERROR(iter.status());
            total_rows += num_rows;
        }

        RETURN_IF_ERROR(builder.finalize());

        auto file_info = builder.file_info();
        if (!file_info.path.empty()) {
            auto* file_meta = merged_compaction->mutable_lcrm_file();
            file_meta->set_name(file_info.path);
            if (file_info.size.has_value()) {
                file_meta->set_size(file_info.size.value());
            }
            VLOG(1) << "Merged " << lcrm_files.size() << " subtask LCRM files into " << file_info.path
                    << ", tablet=" << tablet_id << ", txn_id=" << txn_id << ", total_rows=" << total_rows
                    << ", size=" << (file_info.size.has_value() ? file_info.size.value() : -1);
        }

        return Status::OK();
    };

    auto st = merge_impl();
    if (!st.ok()) {
        (void)fs::delete_file(output_path);
    }
    return st;
}

} // namespace starrocks::lake
