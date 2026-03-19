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

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "gen_cpp/lake_service.pb.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/rowset.h"

namespace starrocks {
class ThreadPool;
}

// Token callbacks for Limiter integration
// AcquireTokenFunc: Returns true if a token was acquired, false otherwise
using AcquireTokenFunc = std::function<bool()>;
// ReleaseTokenFunc: Releases a token, parameter indicates if memory limit was exceeded
using ReleaseTokenFunc = std::function<void(bool mem_limit_exceeded)>;

namespace starrocks::lake {

class TabletManager;
class CompactionTaskCallback;
struct CompactionTaskInfo;

// Subtask type for parallel compaction
enum class SubtaskType {
    NORMAL,           // Normal subtask containing multiple complete rowsets
    LARGE_ROWSET_PART // Subtask that is part of a large rowset split
};

// Group of rowsets/segments for a single subtask
struct SubtaskGroup {
    SubtaskType type = SubtaskType::NORMAL;

    // For NORMAL type: multiple complete rowsets
    std::vector<RowsetPtr> rowsets;

    // For LARGE_ROWSET_PART type: single rowset with segment range
    RowsetPtr large_rowset;
    uint32_t large_rowset_id = 0;
    int32_t segment_start = 0;
    int32_t segment_end = 0;

    // Total data size of this group
    int64_t total_bytes = 0;
};

// Subtask info for tracking parallel compaction progress
struct SubtaskInfo {
    int32_t subtask_id = 0;
    std::vector<uint32_t> input_rowset_ids;
    int64_t input_bytes = 0;
    int64_t start_time = 0;
    // Pointer to the running context (valid only during execution)
    // Used to get real-time progress and status in list_tasks()
    CompactionTaskContext* context = nullptr;
    // Subtask type
    SubtaskType type = SubtaskType::NORMAL;
    // For LARGE_ROWSET_PART type: the original large rowset ID
    uint32_t large_rowset_id = 0;
    // For LARGE_ROWSET_PART type: segment range [segment_start, segment_end)
    int32_t segment_start = 0;
    int32_t segment_end = 0;
};

// Single tablet's parallel compaction state
struct TabletParallelCompactionState {
    int64_t tablet_id = 0;
    int64_t txn_id = 0;
    int64_t version = 0;

    // Rowsets currently being compacted (to avoid conflicts)
    // Key: rowset_id, Value: reference count (number of subtasks using this rowset)
    // For large rowset split, multiple subtasks share the same rowset_id, so we use
    // reference counting instead of a simple set to ensure the rowset is only unmarked
    // when ALL subtasks using it have completed.
    std::unordered_map<uint32_t, int> compacting_rowsets;

    // Running subtasks
    std::unordered_map<int32_t, SubtaskInfo> running_subtasks;

    // Completed subtask contexts
    std::vector<std::unique_ptr<CompactionTaskContext>> completed_subtasks;

    // Configuration
    int32_t max_parallel = 0;
    int64_t max_bytes_per_subtask = 0;

    // Next subtask ID
    int32_t next_subtask_id = 0;

    // Total number of subtasks created
    int32_t total_subtasks_created = 0;

    // Callback for the original compaction request
    std::shared_ptr<CompactionTaskCallback> callback;

    // Callback to release limiter token when subtask completes
    ReleaseTokenFunc release_token;

    // For large rowset split: map from original rowset_id to list of subtask_ids
    // All subtasks in the same group must succeed for the large rowset compaction to be applied
    std::unordered_map<uint32_t, std::vector<int32_t>> large_rowset_split_groups;

    // Expected number of subtasks for each large rowset split.
    // This is recorded BEFORE subtask creation to detect incomplete splits
    // (when acquire_token() fails after some subtasks are created).
    // If large_rowset_split_groups[rowset_id].size() != expected_large_rowset_split_counts[rowset_id],
    // the split is incomplete and should be treated as failed.
    std::unordered_map<uint32_t, int32_t> expected_large_rowset_split_counts;

    // Mutex for thread-safe access
    mutable std::mutex mutex;

    // Check if we can create a new subtask
    bool can_create_subtask() const { return running_subtasks.size() < static_cast<size_t>(max_parallel); }

    // Check if a rowset is being compacted (reference count > 0)
    bool is_rowset_compacting(uint32_t rowset_id) const {
        auto it = compacting_rowsets.find(rowset_id);
        return it != compacting_rowsets.end() && it->second > 0;
    }

    // Check if all subtasks are completed
    bool is_complete() const { return running_subtasks.empty() && total_subtasks_created > 0; }
};

// Manager for per-tablet parallel compaction
// This enables running multiple compaction tasks concurrently within a single tablet
// by selecting non-overlapping rowset groups for each subtask.
class TabletParallelCompactionManager {
public:
    explicit TabletParallelCompactionManager(TabletManager* tablet_mgr);
    ~TabletParallelCompactionManager();

    // Create parallel compaction tasks for a tablet
    // Returns the number of subtasks created
    StatusOr<int> create_parallel_tasks(int64_t tablet_id, int64_t txn_id, int64_t version,
                                        const TabletParallelConfig& config,
                                        std::shared_ptr<CompactionTaskCallback> callback, bool force_base_compaction,
                                        ThreadPool* thread_pool, const AcquireTokenFunc& acquire_token,
                                        const ReleaseTokenFunc& release_token);

    // Get tablet's parallel state (for testing/monitoring)
    // Returns shared_ptr to ensure the state remains valid while being used.
    std::shared_ptr<TabletParallelCompactionState> get_tablet_state(int64_t tablet_id, int64_t txn_id);

    // Subtask completion callback
    void on_subtask_complete(int64_t tablet_id, int64_t txn_id, int32_t subtask_id,
                             std::unique_ptr<CompactionTaskContext> context);

    // Check if all subtasks for a tablet are complete
    bool is_tablet_complete(int64_t tablet_id, int64_t txn_id);

    // Cleanup tablet state after all subtasks complete
    void cleanup_tablet(int64_t tablet_id, int64_t txn_id);

    // Get merged TxnLog from all completed subtasks
    StatusOr<TxnLogPB> get_merged_txn_log(int64_t tablet_id, int64_t txn_id);

    // Metrics
    int64_t running_subtasks() const { return _running_subtasks.load(); }
    int64_t completed_subtasks() const { return _completed_subtasks.load(); }

    // List all running parallel compaction tasks for monitoring
    // Forward declaration of CompactionTaskInfo is in compaction_scheduler.h
    void list_tasks(std::vector<CompactionTaskInfo>* infos);

    // Test-only: Register a pre-created tablet state for unit testing
    // This allows tests to bypass the normal create_parallel_tasks flow
    void register_tablet_state_for_test(int64_t tablet_id, int64_t txn_id,
                                        std::shared_ptr<TabletParallelCompactionState> state) {
        std::lock_guard<std::mutex> lock(_states_mutex);
        std::string key = make_state_key(tablet_id, txn_id);
        _tablet_states[key] = std::move(state);
    }

private:
    // Mark rowsets as being compacted
    void mark_rowsets_compacting(TabletParallelCompactionState* state, const std::vector<uint32_t>& rowset_ids);

    // Unmark rowsets after compaction completes
    void unmark_rowsets_compacting(TabletParallelCompactionState* state, const std::vector<uint32_t>& rowset_ids);

    // Execute a single subtask
    void execute_subtask(int64_t tablet_id, int64_t txn_id, int32_t subtask_id, std::vector<RowsetPtr> input_rowsets,
                         int64_t version, bool force_base_compaction, const ReleaseTokenFunc& release_token);

    // Execute SST compaction once after all subtasks complete
    // This is called from get_merged_txn_log to perform unified SST compaction
    // Results are stored in OpParallelCompaction instead of OpCompaction
    Status execute_sst_compaction_for_parallel(int64_t tablet_id, int64_t version,
                                               TxnLogPB_OpParallelCompaction* op_parallel);

    // Pick rowsets for compaction using CompactionPolicy
    StatusOr<std::vector<RowsetPtr>> pick_rowsets_for_compaction(int64_t tablet_id, int64_t txn_id, int64_t version,
                                                                 bool force_base_compaction);

    // Split rowsets into groups for parallel compaction
    // Returns empty vector if no valid groups can be formed
    // is_pk_table: if true, skip adjacency gap detection (PK tables don't require adjacent rowsets)
    std::vector<std::vector<RowsetPtr>> split_rowsets_into_groups(int64_t tablet_id, std::vector<RowsetPtr> all_rowsets,
                                                                  int32_t max_parallel, int64_t max_bytes,
                                                                  bool is_pk_table);

    // Create and register tablet state for parallel compaction
    // Returns nullptr if state already exists
    StatusOr<std::shared_ptr<TabletParallelCompactionState>> create_and_register_tablet_state(
            int64_t tablet_id, int64_t txn_id, int64_t version, int32_t max_parallel, int64_t max_bytes,
            std::shared_ptr<CompactionTaskCallback> callback, const ReleaseTokenFunc& release_token);

    // Submit subtasks to thread pool
    // Returns the number of subtasks successfully submitted
    StatusOr<int> submit_subtasks(const std::shared_ptr<TabletParallelCompactionState>& state_ptr,
                                  std::vector<std::vector<RowsetPtr>> groups, bool force_base_compaction,
                                  ThreadPool* thread_pool, const AcquireTokenFunc& acquire_token,
                                  const ReleaseTokenFunc& release_token);

    // Submit subtasks from SubtaskGroup to thread pool (new API for large rowset split)
    // Returns the number of subtasks successfully submitted
    StatusOr<int> submit_subtasks_from_groups(const std::shared_ptr<TabletParallelCompactionState>& state_ptr,
                                              std::vector<SubtaskGroup> groups, bool force_base_compaction,
                                              ThreadPool* thread_pool, const AcquireTokenFunc& acquire_token,
                                              const ReleaseTokenFunc& release_token);

    // Execute a single subtask for large rowset split (segment range mode)
    void execute_subtask_segment_range(int64_t tablet_id, int64_t txn_id, int32_t subtask_id,
                                       const RowsetPtr& input_rowset, uint32_t large_rowset_id, int32_t segment_start,
                                       int32_t segment_end, int64_t version, bool force_base_compaction,
                                       const ReleaseTokenFunc& release_token);

    // Generate state key from tablet_id and txn_id
    static std::string make_state_key(int64_t tablet_id, int64_t txn_id) {
        return std::to_string(tablet_id) + "_" + std::to_string(txn_id);
    }

    // ================================================================================
    // Helper structures and functions for split_rowsets_into_groups
    // ================================================================================

    // Statistics about rowsets for planning parallel compaction
    struct RowsetStats {
        int64_t total_bytes = 0;
        int64_t total_segments = 0;
        bool has_delete_predicate = false;
    };

    // Configuration for grouping algorithm
    struct GroupingConfig {
        int32_t num_subtasks = 1;
        int64_t target_bytes_per_subtask = 0;
        size_t target_rowsets_per_subtask = 2;
        bool skip_excess_data = false;
        // Maximum segments per subtask for non-PK tables to avoid partial compaction.
        // Only applies when enable_lake_compaction_use_partial_segments is true.
        // 0 means no limit.
        int64_t max_segments_per_subtask = 0;
    };

    // Check if a group is valid for compaction
    static bool _is_group_valid_for_compaction(const std::vector<RowsetPtr>& group);

    // Filter out large non-overlapped rowsets that don't need compaction
    static std::vector<RowsetPtr> _filter_compactable_rowsets(int64_t tablet_id, std::vector<RowsetPtr> all_rowsets);

    // Calculate statistics about rowsets
    static RowsetStats _calculate_rowset_stats(const std::vector<RowsetPtr>& rowsets);

    // Calculate optimal grouping configuration
    static GroupingConfig _calculate_grouping_config(int64_t tablet_id, const std::vector<RowsetPtr>& rowsets,
                                                     const RowsetStats& stats, int32_t max_parallel, int64_t max_bytes);

    // Group rowsets into subtasks based on the configuration
    static std::vector<std::vector<RowsetPtr>> _group_rowsets_into_subtasks(int64_t tablet_id,
                                                                            std::vector<RowsetPtr> all_rowsets,
                                                                            const GroupingConfig& config,
                                                                            int64_t max_bytes, bool is_pk_table);

    // Filter out invalid groups that cannot be compacted
    static std::vector<std::vector<RowsetPtr>> _filter_invalid_groups(int64_t tablet_id,
                                                                      std::vector<std::vector<RowsetPtr>> groups);

    // ================================================================================
    // Large rowset split related functions (all table types)
    // ================================================================================

    // Check if a rowset should be split into multiple subtasks
    // Criteria: data_size >= 2 * lake_compaction_max_rowset_size AND
    //           data_size > max_bytes_per_subtask AND
    //           segments_size >= 4 AND is_overlapped
    static bool _is_large_rowset_for_split(const RowsetPtr& rowset, int64_t max_bytes_per_subtask);

    // Split a large rowset into multiple SubtaskGroups based on segment data size.
    // Each group contains a segment range [start, end) with approximate target_bytes_per_subtask.
    // If max_subtasks > 0 and the rowset would need more subtasks than max_subtasks,
    // it caps the split to max_subtasks (allowing each subtask to exceed target_bytes_per_subtask).
    // This ensures the large rowset split is always complete and avoids data loss.
    // Note: max_subtasks should be >= 2 for meaningful splitting; the caller should skip
    // splitting if only 1 slot is available.
    static std::vector<SubtaskGroup> _split_large_rowset(const RowsetPtr& rowset, int64_t target_bytes_per_subtask,
                                                         int32_t max_subtasks = 0);

    // Group small rowsets into SubtaskGroups
    // Each group contains multiple complete rowsets with total size <= target_bytes_per_subtask
    static std::vector<SubtaskGroup> _group_small_rowsets(std::vector<RowsetPtr> rowsets,
                                                          int64_t target_bytes_per_subtask);

    // Create SubtaskGroups from selected rowsets (main entry for all table types)
    // This handles both large rowset splitting and small rowset grouping
    std::vector<SubtaskGroup> _create_subtask_groups(int64_t tablet_id, std::vector<RowsetPtr> rowsets,
                                                     int32_t max_parallel, int64_t max_bytes_per_subtask);

    // Merge multiple subtask LCRM files into a single LCRM file for the merged compaction.
    // This enables the light publish path (SST ingestion) for large rowset split compaction.
    Status _merge_subtask_lcrm_files(int64_t tablet_id, int64_t txn_id, const std::vector<FileMetaPB>& lcrm_files,
                                     const std::vector<int64_t>& num_rows_per_subtask,
                                     TxnLogPB_OpCompaction* merged_compaction);

    TabletManager* _tablet_mgr;

    // State map: key = tablet_id_txn_id
    // Using shared_ptr to allow safe access from on_subtask_complete even if cleanup_tablet
    // concurrently removes the state from the map.
    std::unordered_map<std::string, std::shared_ptr<TabletParallelCompactionState>> _tablet_states;
    mutable std::mutex _states_mutex;

    // Metrics
    std::atomic<int64_t> _running_subtasks{0};
    std::atomic<int64_t> _completed_subtasks{0};
};

} // namespace starrocks::lake
