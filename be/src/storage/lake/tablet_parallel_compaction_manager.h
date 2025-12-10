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

namespace starrocks::lake {

class TabletManager;
class CompactionTaskCallback;

// Subtask info for tracking parallel compaction progress
struct SubtaskInfo {
    int32_t subtask_id;
    std::vector<uint32_t> input_rowset_ids;
    int64_t input_bytes = 0;
    int64_t start_time = 0;
    int progress = 0;
};

// Single tablet's parallel compaction state
struct TabletParallelState {
    int64_t tablet_id = 0;
    int64_t txn_id = 0;
    int64_t version = 0;
    int64_t table_id = 0;      // Table ID for tablet write log
    int64_t partition_id = 0;  // Partition ID for tablet write log

    // Rowsets currently being compacted (to avoid conflicts)
    std::unordered_set<uint32_t> compacting_rowsets;

    // Running subtasks
    std::unordered_map<int32_t, SubtaskInfo> running_subtasks;

    // Completed subtask contexts
    std::vector<std::unique_ptr<CompactionTaskContext>> completed_subtasks;

    // Configuration
    int32_t max_parallel = 3;
    int64_t max_bytes_per_subtask = 10737418240L; // 10GB

    // Next subtask ID
    int32_t next_subtask_id = 0;

    // Total number of subtasks created
    int32_t total_subtasks_created = 0;

    // Callback for the original compaction request
    std::shared_ptr<CompactionTaskCallback> callback;

    // Mutex for thread-safe access
    mutable std::mutex mutex;

    // Check if we can create a new subtask
    bool can_create_subtask() const { return running_subtasks.size() < static_cast<size_t>(max_parallel); }

    // Check if a rowset is being compacted
    bool is_rowset_compacting(uint32_t rowset_id) const { return compacting_rowsets.count(rowset_id) > 0; }

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
                                        ThreadPool* thread_pool, int64_t table_id = 0, int64_t partition_id = 0);

    // Get tablet's parallel state (for testing/monitoring)
    TabletParallelState* get_tablet_state(int64_t tablet_id, int64_t txn_id);

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

private:
    // Pick rowsets that don't conflict with currently compacting ones
    StatusOr<std::vector<RowsetPtr>> pick_non_conflicting_rowsets(int64_t tablet_id, int64_t version, int64_t max_bytes,
                                                                  const std::unordered_set<uint32_t>& exclude_rowsets,
                                                                  bool force_base_compaction);

    // Mark rowsets as being compacted
    void mark_rowsets_compacting(TabletParallelState* state, const std::vector<uint32_t>& rowset_ids);

    // Unmark rowsets after compaction completes
    void unmark_rowsets_compacting(TabletParallelState* state, const std::vector<uint32_t>& rowset_ids);

    // Execute a single subtask
    void execute_subtask(int64_t tablet_id, int64_t txn_id, int32_t subtask_id, std::vector<RowsetPtr> input_rowsets,
                         int64_t version, bool force_base_compaction);

    // Generate state key from tablet_id and txn_id
    static std::string make_state_key(int64_t tablet_id, int64_t txn_id) {
        return std::to_string(tablet_id) + "_" + std::to_string(txn_id);
    }

    TabletManager* _tablet_mgr;

    // State map: key = tablet_id_txn_id
    std::unordered_map<std::string, std::unique_ptr<TabletParallelState>> _tablet_states;
    mutable std::mutex _states_mutex;

    // Metrics
    std::atomic<int64_t> _running_subtasks{0};
    std::atomic<int64_t> _completed_subtasks{0};
};

} // namespace starrocks::lake
