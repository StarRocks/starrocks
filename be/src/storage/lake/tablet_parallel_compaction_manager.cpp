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

#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/lake_types.pb.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/memtable_flush_executor.h"
#include "storage/storage_engine.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace starrocks::lake {

namespace {

// Collect successful subtask IDs (unified logic for both PK and non-PK tables)
// All successful subtasks are included regardless of whether they are consecutive.
// Each successful subtask's output rowset will replace its corresponding input rowsets.
std::vector<int32_t> collect_success_subtask_ids(
        const std::vector<std::unique_ptr<CompactionTaskContext>>& completed_subtasks, int64_t tablet_id,
        int64_t txn_id) {
    std::vector<int32_t> success_subtask_ids;
    for (const auto& ctx : completed_subtasks) {
        if (!ctx->status.ok()) {
            LOG(WARNING) << "Parallel compaction: skipping failed subtask " << ctx->subtask_id << " for tablet "
                         << tablet_id << ", txn_id=" << txn_id << ", status=" << ctx->status;
            continue;
        }
        success_subtask_ids.push_back(ctx->subtask_id);
    }
    return success_subtask_ids;
}

} // namespace

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

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " planning " << config.num_subtasks
            << " subtasks with target_bytes_per_subtask=" << config.target_bytes_per_subtask
            << " target_rowsets_per_subtask=" << config.target_rowsets_per_subtask
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
    size_t skipped_rowsets = 0;
    int64_t skipped_bytes = 0;
    int32_t last_rowset_index = -1;

    for (size_t rowset_idx = 0; rowset_idx < all_rowsets.size(); rowset_idx++) {
        auto& rowset = all_rowsets[rowset_idx];
        int64_t rowset_bytes = rowset->data_size();
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

        if (should_start_new_group) {
            std::vector<uint32_t> group_ids;
            for (const auto& r : current_group) {
                group_ids.push_back(r->id());
            }
            std::string reason = has_adjacency_gap ? " (adjacency gap)" : "";
            VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " group " << valid_groups.size() << ": "
                    << current_group.size() << " rowsets, " << current_bytes << " bytes, ids=["
                    << JoinInts(group_ids, ",") << "]" << reason;

            valid_groups.push_back(std::move(current_group));
            current_group.clear();
            current_bytes = 0;
        }

        current_group.push_back(std::move(rowset));
        current_bytes += rowset_bytes;
        last_rowset_index = current_rowset_index;
    }

    // Add the last group if not empty
    if (!current_group.empty()) {
        std::vector<uint32_t> group_ids;
        for (const auto& r : current_group) {
            group_ids.push_back(r->id());
        }
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " group " << valid_groups.size() << ": "
                << current_group.size() << " rowsets, " << current_bytes << " bytes, ids=[" << JoinInts(group_ids, ",")
                << "]";
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

    // Step 1: Pick rowsets for compaction
    ASSIGN_OR_RETURN(auto all_rowsets, pick_rowsets_for_compaction(tablet_id, txn_id, version, force_base_compaction));
    size_t total_rowsets_count = all_rowsets.size();

    // Step 2: Split rowsets into groups
    auto valid_groups =
            split_rowsets_into_groups(tablet_id, std::move(all_rowsets), max_parallel, max_bytes, is_pk_table);

    if (valid_groups.empty()) {
        // Empty groups means we should fallback to normal compaction flow.
        // This happens when:
        // 1. Total data size is less than max_bytes_per_subtask (no parallelism benefit)
        // 2. Not enough rowsets for parallel compaction
        // 3. Has delete predicate
        // 4. max_parallel <= 1
        // Return 0 to indicate fallback to normal compaction (not an error).
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " txn=" << txn_id
                << " fallback to normal compaction, total_rowsets=" << total_rowsets_count;
        return 0;
    }

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " created " << valid_groups.size() << " groups from "
            << total_rowsets_count << " rowsets";

    // Step 3: Create and register tablet state
    ASSIGN_OR_RETURN(auto state_ptr, create_and_register_tablet_state(tablet_id, txn_id, version, max_parallel,
                                                                      max_bytes, std::move(callback), release_token));

    // Step 4: Submit subtasks
    return submit_subtasks(std::move(state_ptr), std::move(valid_groups), force_base_compaction, thread_pool,
                           acquire_token, release_token);
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

        callback->finish_task(std::move(merged_context));

        // Cleanup
        cleanup_tablet(tablet_id, txn_id);
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

    auto* op_compaction = merged_log.mutable_op_compaction();

    // Variables to be used outside the lock
    int64_t version = 0;
    std::vector<int32_t> success_subtask_ids;

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

        // Collect successful subtask IDs (unified logic for both PK and non-PK tables)
        // All successful subtasks are included regardless of whether they are consecutive.
        success_subtask_ids = collect_success_subtask_ids(state->completed_subtasks, tablet_id, txn_id);

        // If no successful subtasks, return error
        if (success_subtask_ids.empty()) {
            return Status::InternalError(strings::Substitute(
                    "All subtasks failed for parallel compaction: tablet_id=$0, txn_id=$1, total_subtasks=$2",
                    tablet_id, txn_id, state->completed_subtasks.size()));
        }

        std::unordered_set<int32_t> success_subtask_id_set(success_subtask_ids.begin(), success_subtask_ids.end());

        // Fill subtask_outputs: each successful subtask generates an independent output rowset
        // Each subtask's output replaces its own input rowsets during publish
        for (const auto& ctx : state->completed_subtasks) {
            if (success_subtask_id_set.count(ctx->subtask_id) == 0) {
                continue;
            }
            if (ctx->txn_log == nullptr || !ctx->txn_log->has_op_compaction()) {
                continue;
            }

            auto* subtask_output = op_compaction->add_subtask_outputs();
            subtask_output->set_subtask_id(ctx->subtask_id);

            // Copy input rowsets from this subtask
            const auto& sub_compaction = ctx->txn_log->op_compaction();
            for (uint32_t rid : sub_compaction.input_rowsets()) {
                subtask_output->add_input_rowsets(rid);
            }

            // Copy output rowset from this subtask
            if (sub_compaction.has_output_rowset()) {
                subtask_output->mutable_output_rowset()->CopyFrom(sub_compaction.output_rowset());
            }

            // Copy compact_version if set
            if (sub_compaction.has_compact_version() && !op_compaction->has_compact_version()) {
                op_compaction->set_compact_version(sub_compaction.compact_version());
            }
        }

        int32_t subtask_count = static_cast<int32_t>(state->completed_subtasks.size());

        // Record subtask_count in txn_log for light_publish to read multiple rows_mapper files
        if (subtask_count > 0) {
            op_compaction->set_subtask_count(subtask_count);
        }

        // Record success_subtask_ids for partial success scenarios
        for (int32_t id : success_subtask_ids) {
            op_compaction->add_success_subtask_ids(id);
        }

        size_t failed_count = state->completed_subtasks.size() - success_subtask_ids.size();
        if (failed_count > 0) {
            VLOG(1) << "Parallel compaction partial success: tablet=" << tablet_id << ", txn_id=" << txn_id
                    << ", successful=" << success_subtask_ids.size() << ", failed=" << failed_count
                    << ", success_subtask_ids=[" << JoinInts(success_subtask_ids, ",") << "]";
        }

        VLOG(1) << "Merged TxnLog for tablet " << tablet_id << ", txn_id=" << txn_id << ", subtasks=" << subtask_count
                << ", successful_subtasks=" << success_subtask_ids.size()
                << ", subtask_outputs=" << op_compaction->subtask_outputs_size();
    }
    // Lock released here

    // Execute SST compaction once after all subtasks complete.
    // This is more efficient than having each subtask independently try to compact SST files,
    // which could lead to conflicts and redundant work.
    RETURN_IF_ERROR(execute_sst_compaction(tablet_id, version, &merged_log));

    return merged_log;
}

void TabletParallelCompactionManager::mark_rowsets_compacting(TabletParallelCompactionState* state,
                                                              const std::vector<uint32_t>& rowset_ids) {
    // Assumes state->mutex is already held
    for (uint32_t rid : rowset_ids) {
        state->compacting_rowsets.insert(rid);
    }
}

void TabletParallelCompactionManager::unmark_rowsets_compacting(TabletParallelCompactionState* state,
                                                                const std::vector<uint32_t>& rowset_ids) {
    // Assumes state->mutex is already held
    for (uint32_t rid : rowset_ids) {
        state->compacting_rowsets.erase(rid);
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

Status TabletParallelCompactionManager::execute_sst_compaction(int64_t tablet_id, int64_t version,
                                                               TxnLogPB* merged_log) {
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

    auto st = update_mgr->execute_index_major_compaction(metadata, merged_log);
    if (!st.ok()) {
        LOG(WARNING) << "SST compaction failed for tablet " << tablet_id << ": " << st
                     << ". This will not fail the parallel compaction.";
        // Don't fail the entire parallel compaction for SST compaction failure
        // SST compaction can be retried in the next compaction cycle
        return Status::OK();
    }

    // Log SST compaction results
    if (merged_log->has_op_compaction() && !merged_log->op_compaction().input_sstables().empty()) {
        size_t total_input_sst_size = 0;
        for (const auto& input_sst : merged_log->op_compaction().input_sstables()) {
            total_input_sst_size += input_sst.filesize();
        }
        VLOG(1) << "SST compaction completed for tablet " << tablet_id
                << ", input_ssts=" << merged_log->op_compaction().input_sstables_size()
                << ", input_size=" << total_input_sst_size;
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

} // namespace starrocks::lake
