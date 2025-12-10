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

namespace starrocks {

// Helper struct to hold merged output rowset statistics
struct MergedOutputStats {
    int64_t total_num_rows = 0;
    int64_t total_data_size = 0;
    bool any_overlapped = false;
    bool has_any_output = false;
};

// Collect successful subtask IDs for PK tables (partial success allowed)
std::vector<int32_t> collect_success_subtask_ids_for_pk_table(
        const std::vector<std::unique_ptr<starrocks::lake::CompactionTaskContext>>& completed_subtasks,
        int64_t tablet_id, int64_t txn_id) {
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

// Collect successful subtask IDs for non-PK tables (longest consecutive run only)
// This ensures we maximize the use of successful work while maintaining correct rowset ordering.
std::vector<int32_t> collect_success_subtask_ids_for_non_pk_table(
        const std::vector<std::unique_ptr<starrocks::lake::CompactionTaskContext>>& completed_subtasks,
        int64_t tablet_id, int64_t txn_id) {
    std::vector<int32_t> success_subtask_ids;

    int32_t best_start = -1;
    int32_t best_length = 0;
    int32_t current_start = -1;
    int32_t current_length = 0;

    for (size_t i = 0; i < completed_subtasks.size(); i++) {
        const auto& ctx = completed_subtasks[i];
        if (ctx->status.ok()) {
            if (current_length == 0) {
                current_start = static_cast<int32_t>(i);
            }
            current_length++;
        } else {
            // End of current run, check if it's the best
            if (current_length > best_length) {
                best_start = current_start;
                best_length = current_length;
            }
            current_length = 0;
            LOG(WARNING) << "Parallel compaction: failed subtask " << ctx->subtask_id << " for non-PK tablet "
                         << tablet_id << ", txn_id=" << txn_id << ", status=" << ctx->status;
        }
    }
    // Check the last run
    if (current_length > best_length) {
        best_start = current_start;
        best_length = current_length;
    }

    // Collect the longest consecutive successful subtask IDs
    if (best_length > 0) {
        for (int32_t i = best_start; i < best_start + best_length; i++) {
            success_subtask_ids.push_back(completed_subtasks[i]->subtask_id);
        }
        if (best_start > 0 || best_length < static_cast<int32_t>(completed_subtasks.size())) {
            VLOG(1) << "Parallel compaction: non-PK tablet " << tablet_id << ", txn_id=" << txn_id
                    << " using longest consecutive run: start=" << best_start << ", length=" << best_length
                    << ", subtask_ids=[" << JoinInts(success_subtask_ids, ",") << "]";
        }
    }

    return success_subtask_ids;
}

// Collect input rowsets from successful subtasks (deduplicated and sorted)
std::set<uint32_t> collect_input_rowsets(
        const std::vector<std::unique_ptr<starrocks::lake::CompactionTaskContext>>& completed_subtasks,
        const std::unordered_set<int32_t>& success_subtask_id_set) {
    std::set<uint32_t> input_rowset_set;
    for (const auto& ctx : completed_subtasks) {
        if (success_subtask_id_set.count(ctx->subtask_id) == 0) {
            continue;
        }
        if (ctx->txn_log != nullptr && ctx->txn_log->has_op_compaction()) {
            for (uint32_t rid : ctx->txn_log->op_compaction().input_rowsets()) {
                input_rowset_set.insert(rid);
            }
        }
    }
    return input_rowset_set;
}

// Merge output rowsets from successful subtasks into a single output rowset
MergedOutputStats merge_output_rowsets(
        const std::vector<std::unique_ptr<starrocks::lake::CompactionTaskContext>>& completed_subtasks,
        const std::unordered_set<int32_t>& success_subtask_id_set, RowsetMetadataPB* merged_output_rowset) {
    MergedOutputStats stats;

    for (const auto& ctx : completed_subtasks) {
        if (success_subtask_id_set.count(ctx->subtask_id) == 0) {
            continue;
        }

        if (ctx->txn_log != nullptr && ctx->txn_log->has_op_compaction()) {
            const auto& sub_compaction = ctx->txn_log->op_compaction();
            if (sub_compaction.has_output_rowset()) {
                const auto& sub_output = sub_compaction.output_rowset();
                stats.has_any_output = true;

                // Merge segments
                for (const auto& segment : sub_output.segments()) {
                    merged_output_rowset->add_segments(segment);
                }

                // Merge segment sizes
                for (uint64_t seg_size : sub_output.segment_size()) {
                    merged_output_rowset->add_segment_size(seg_size);
                }

                // Merge segment encryption metas
                for (const auto& enc_meta : sub_output.segment_encryption_metas()) {
                    merged_output_rowset->add_segment_encryption_metas(enc_meta);
                }

                // Accumulate statistics
                stats.total_num_rows += sub_output.num_rows();
                stats.total_data_size += sub_output.data_size();

                if (sub_output.overlapped()) {
                    stats.any_overlapped = true;
                }
            }
        }
    }
    return stats;
}

// Set compact_version from the first successful subtask
void set_compact_version(const std::vector<std::unique_ptr<starrocks::lake::CompactionTaskContext>>& completed_subtasks,
                         const std::unordered_set<int32_t>& success_subtask_id_set,
                         TxnLogPB_OpCompaction* op_compaction) {
    for (const auto& ctx : completed_subtasks) {
        if (success_subtask_id_set.count(ctx->subtask_id) > 0 && ctx->txn_log != nullptr &&
            ctx->txn_log->has_op_compaction()) {
            const auto& first_compaction = ctx->txn_log->op_compaction();
            if (first_compaction.has_compact_version()) {
                op_compaction->set_compact_version(first_compaction.compact_version());
                break;
            }
        }
    }
}

} // namespace starrocks

namespace starrocks::lake {

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
        int64_t tablet_id, std::vector<RowsetPtr> all_rowsets, int32_t max_parallel, int64_t max_bytes) {
    std::vector<std::vector<RowsetPtr>> valid_groups;

    // Check if any rowset has delete predicate - if so, disable parallelism.
    // Delete predicates must be applied to ALL prior rowsets during base compaction.
    // If we split rowsets into parallel subtasks, delete predicates won't be correctly
    // applied because each subtask only sees a subset of the data.
    // For example:
    //   rowset_0 (data), rowset_1 (data), rowset_2 (DELETE WHERE key < 3), rowset_3 (data)
    // If we split into [rowset_0, rowset_1] and [rowset_2, rowset_3]:
    //   - The first subtask won't know about the delete predicate
    //   - Data with key < 3 in rowset_0 and rowset_1 won't be deleted
    bool has_delete_predicate = false;
    for (const auto& r : all_rowsets) {
        if (r->metadata().has_delete_predicate()) {
            has_delete_predicate = true;
            break;
        }
    }

    // Calculate total bytes
    int64_t total_bytes = 0;
    for (const auto& r : all_rowsets) {
        total_bytes += r->data_size();
    }

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " total_rowsets=" << all_rowsets.size()
            << " total_bytes=" << total_bytes << " max_bytes_per_subtask=" << max_bytes
            << " has_delete_predicate=" << has_delete_predicate;

    // If total bytes is small enough, max_parallel is 1, or has delete predicate, use a single group
    if (total_bytes <= max_bytes || max_parallel <= 1 || has_delete_predicate) {
        std::vector<uint32_t> group_ids;
        for (const auto& r : all_rowsets) {
            group_ids.push_back(r->id());
        }
        std::string reason = has_delete_predicate ? "has_delete_predicate"
                                                  : (max_parallel <= 1) ? "max_parallel<=1" : "data_size_small";
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " using single group (" << reason
                << "): " << all_rowsets.size() << " rowsets, " << total_bytes << " bytes, ids=["
                << JoinInts(group_ids, ",") << "]";
        valid_groups.push_back(std::move(all_rowsets));
        return valid_groups;
    }

    // Calculate optimal number of subtasks using ceiling division
    int32_t num_subtasks = static_cast<int32_t>((total_bytes + max_bytes - 1) / max_bytes);

    // Check if we need to skip some data due to max_parallel limit
    bool skip_excess_data = num_subtasks > max_parallel;
    if (skip_excess_data) {
        num_subtasks = max_parallel;
        VLOG(1) << "Parallel compaction: tablet=" << tablet_id
                << " data exceeds max_parallel capacity, will skip excess data for later compaction"
                << ", max_parallel=" << max_parallel << ", would need=" << ((total_bytes + max_bytes - 1) / max_bytes);
    }

    // Calculate target bytes per subtask for even distribution
    int64_t processable_bytes = skip_excess_data ? (static_cast<int64_t>(max_parallel) * max_bytes) : total_bytes;
    int64_t target_bytes_per_subtask = processable_bytes / num_subtasks;

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " planning " << num_subtasks
            << " subtasks with target_bytes_per_subtask=" << target_bytes_per_subtask
            << " (processable_bytes=" << processable_bytes << ")";

    // Split rowsets into groups with balanced distribution
    std::vector<RowsetPtr> current_group;
    int64_t current_bytes = 0;
    size_t skipped_rowsets = 0;
    int64_t skipped_bytes = 0;

    for (size_t rowset_idx = 0; rowset_idx < all_rowsets.size(); rowset_idx++) {
        auto& rowset = all_rowsets[rowset_idx];
        int64_t rowset_bytes = rowset->data_size();

        // Check if we should skip this rowset due to max_parallel/max_bytes limits
        if (skip_excess_data) {
            // Case 1: We've already finalized max_parallel groups, skip all remaining
            if (static_cast<int32_t>(valid_groups.size()) >= max_parallel) {
                skipped_rowsets++;
                skipped_bytes += rowset_bytes;
                continue;
            }
            // Case 2: We're building the last allowed group and adding this rowset would exceed max_bytes
            if (static_cast<int32_t>(valid_groups.size()) == max_parallel - 1 && !current_group.empty() &&
                current_bytes + rowset_bytes > max_bytes) {
                skipped_rowsets++;
                skipped_bytes += rowset_bytes;
                continue;
            }
        }

        // Check if we should start a new group
        bool should_start_new_group = !current_group.empty() &&
                                      current_bytes + rowset_bytes > target_bytes_per_subtask &&
                                      static_cast<int32_t>(valid_groups.size()) < num_subtasks - 1;

        if (should_start_new_group) {
            std::vector<uint32_t> group_ids;
            for (const auto& r : current_group) {
                group_ids.push_back(r->id());
            }
            VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " group " << valid_groups.size() << ": "
                    << current_group.size() << " rowsets, " << current_bytes << " bytes, ids=["
                    << JoinInts(group_ids, ",") << "]";

            valid_groups.push_back(std::move(current_group));
            current_group.clear();
            current_bytes = 0;
        }

        current_group.push_back(std::move(rowset));
        current_bytes += rowset_bytes;
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
                << skipped_bytes << " bytes) for later compaction due to max_parallel limit";
    }

    return valid_groups;
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
        LOG(WARNING) << "Parallel compaction: failed to create any subtask for tablet " << tablet_id
                     << ", txn_id=" << txn_id << ", falling back to non-parallel mode";
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
    // Validate configuration (use FE config, proto has default values)
    int32_t max_parallel = config.max_parallel_per_tablet();
    int64_t max_bytes = config.max_bytes_per_subtask();

    if (max_parallel <= 0 || max_bytes <= 0) {
        return Status::InvalidArgument(
                strings::Substitute("Invalid parallel compaction configuration: tablet_id=$0, txn_id=$1, version=$2, "
                                    "max_parallel=$3, max_bytes=$4, force_base_compaction=$5",
                                    tablet_id, txn_id, version, max_parallel, max_bytes, force_base_compaction));
    }

    VLOG(1) << "Parallel compaction: tablet=" << tablet_id << " txn=" << txn_id << " version=" << version
            << " max_parallel=" << max_parallel << " max_bytes=" << max_bytes;

    // Step 1: Pick rowsets for compaction
    ASSIGN_OR_RETURN(auto all_rowsets, pick_rowsets_for_compaction(tablet_id, txn_id, version, force_base_compaction));
    size_t total_rowsets_count = all_rowsets.size();

    // Step 2: Split rowsets into groups
    auto valid_groups = split_rowsets_into_groups(tablet_id, std::move(all_rowsets), max_parallel, max_bytes);

    if (valid_groups.empty()) {
        // Note: total_bytes is not available here because all_rowsets was moved into split_rowsets_into_groups.
        // This situation should rarely happen since split_rowsets_into_groups returns non-empty groups
        // when input rowsets are non-empty.
        return Status::NotFound(strings::Substitute(
                "No valid rowset groups for parallel compaction: tablet_id=$0, txn_id=$1, version=$2, "
                "total_rowsets=$3, max_parallel=$4, max_bytes_per_subtask=$5",
                tablet_id, txn_id, version, total_rowsets_count, max_parallel, max_bytes));
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
    // separate publish RPC after compaction completes. The files will be cleaned up by:
    // 1. MultiRowsMapperIterator (with set_delete_files_on_close=true) if light_publish is used
    // 2. publish_primary_compaction explicitly if light_publish is NOT used
    // See PrimaryKeyCompactionConflictResolver::execute() and UpdateManager::publish_primary_compaction().

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
    MergedOutputStats output_stats;

    // Get tablet metadata to determine if this is a PK table
    // PK tables can use partial success (any successful subtasks), while non-PK tables
    // must use consecutive prefix success to preserve rowset version ordering.
    bool is_pk_table = false;
    {
        auto tablet_or = _tablet_mgr->get_tablet(tablet_id, state->version);
        if (tablet_or.ok()) {
            const auto& metadata = tablet_or.value().metadata();
            if (metadata && metadata->schema().keys_type() == KeysType::PRIMARY_KEYS) {
                is_pk_table = true;
            }
        }
    }

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

        // Collect successful subtask IDs based on table type
        if (is_pk_table) {
            success_subtask_ids =
                    collect_success_subtask_ids_for_pk_table(state->completed_subtasks, tablet_id, txn_id);
        } else {
            success_subtask_ids =
                    collect_success_subtask_ids_for_non_pk_table(state->completed_subtasks, tablet_id, txn_id);
        }

        // If no successful subtasks, return error
        if (success_subtask_ids.empty()) {
            return Status::InternalError(strings::Substitute(
                    "All subtasks failed for parallel compaction: tablet_id=$0, txn_id=$1, total_subtasks=$2",
                    tablet_id, txn_id, state->completed_subtasks.size()));
        }

        std::unordered_set<int32_t> success_subtask_id_set(success_subtask_ids.begin(), success_subtask_ids.end());

        // Collect input rowsets from successful subtasks (deduplicated and sorted)
        auto input_rowset_set = collect_input_rowsets(state->completed_subtasks, success_subtask_id_set);
        for (uint32_t rid : input_rowset_set) {
            op_compaction->add_input_rowsets(rid);
        }

        // Merge output rowsets from successful subtasks
        auto* merged_output_rowset = op_compaction->mutable_output_rowset();
        output_stats = merge_output_rowsets(state->completed_subtasks, success_subtask_id_set, merged_output_rowset);

        // Set merged statistics if we have any output
        if (output_stats.has_any_output) {
            merged_output_rowset->set_num_rows(output_stats.total_num_rows);
            merged_output_rowset->set_data_size(output_stats.total_data_size);
            // Parallel compaction outputs are overlapped since they come from different subtasks
            bool has_gaps = is_pk_table && (success_subtask_ids.size() < state->completed_subtasks.size());
            merged_output_rowset->set_overlapped(output_stats.any_overlapped || success_subtask_ids.size() > 1 ||
                                                 has_gaps);
        }

        // Set compact_version from first successful subtask
        set_compact_version(state->completed_subtasks, success_subtask_id_set, op_compaction);

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
                    << ", is_pk_table=" << is_pk_table << ", successful=" << success_subtask_ids.size()
                    << ", failed=" << failed_count << ", success_subtask_ids=[" << JoinInts(success_subtask_ids, ",")
                    << "]" << (is_pk_table ? "" : " (non-PK: consecutive prefix only)");
        }

        VLOG(1) << "Merged TxnLog for tablet " << tablet_id << ", txn_id=" << txn_id
                << ", input_rowsets=" << input_rowset_set.size() << ", subtasks=" << subtask_count
                << ", successful_subtasks=" << success_subtask_ids.size()
                << ", output_segments=" << merged_output_rowset->segments_size()
                << ", output_rows=" << output_stats.total_num_rows << ", output_size=" << output_stats.total_data_size;
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
