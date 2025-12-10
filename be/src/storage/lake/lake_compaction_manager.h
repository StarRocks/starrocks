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
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"
#include "storage/lake/compaction_result_manager.h"
#include "storage/lake/compaction_task.h"
#include "util/threadpool.h"

namespace starrocks::lake {

class TabletManager;
class CompactionResultManager;

// Tablet compaction info for priority queue
struct TabletCompactionInfo {
    int64_t tablet_id;
    double score;
    int64_t last_update_time_ms;

    // For priority queue ordering (higher score = higher priority)
    bool operator<(const TabletCompactionInfo& other) const {
        if (score != other.score) {
            return score < other.score; // max heap
        }
        return last_update_time_ms > other.last_update_time_ms; // older first
    }
};

// Tracks compaction state for a single tablet
struct TabletCompactionState {
    int64_t tablet_id;
    // Rowsets currently being compacted
    std::unordered_set<uint32_t> compacting_rowsets;
    // Number of running compaction tasks for this tablet
    int running_tasks = 0;
    // Last calculated compaction score
    double last_score = 0.0;
    // Last update time
    int64_t last_update_time_ms = 0;
    // Whether this tablet is in the priority queue
    bool in_queue = false;
    // Has pending results from previous compaction (recovered from crash)
    bool has_pending_results = false;
};

// Autonomous compaction manager for lake tablets
// Implements event-driven, tablet-level compaction scheduling with:
// - Priority queue based on compaction score
// - Per-tablet parallel task support
// - Rowset-level conflict avoidance
// - Data volume limits per task
//
// Exception Recovery (Design Doc Section 6.3.1):
// This manager leverages existing mechanisms instead of BE startup self-check:
// - Score-Driven Scheduling: Partition score remains high → FE schedules it again
// - Request-Triggered Scheduling: PUBLISH_AUTONOMOUS brings tablet_ids → BE queues them on demand
// - On-Demand Execution: BE only processes partitions FE determines require compaction
class LakeCompactionManager {
public:
    explicit LakeCompactionManager(TabletManager* tablet_mgr);
    ~LakeCompactionManager();

    // Create and initialize the singleton instance
    // Must be called once during system startup
    static Status create_instance(TabletManager* tablet_mgr);

    // Get singleton instance (returns nullptr if not initialized)
    static LakeCompactionManager* instance();

    // Initialize and start the manager (called automatically by create_instance)
    Status init();

    // Stop the manager
    void stop();

    // ============== Event-Driven Scheduling ==============

    // Update a tablet's compaction state (event-driven trigger)
    // This is called when:
    // - Version is published
    // - Manual compaction is triggered
    // - A compaction task completes
    // - PUBLISH_AUTONOMOUS triggers scheduling (Mechanism 3)
    void update_tablet_async(int64_t tablet_id);

    // Batch update for multiple tablets
    // Used by PUBLISH_AUTONOMOUS to trigger scheduling for all tablets in a partition
    void update_tablets_async(const std::vector<int64_t>& tablet_ids);

    // ============== Startup Recovery ==============

    // Recover state from local compaction results after BE restart
    // This implements Mechanism 1: Local Result Recovery
    // Called during BE startup after init()
    Status recover_from_local_results();

    // ============== Query Methods ==============

    // Get current metrics
    int64_t running_tasks() const { return _running_tasks.load(); }
    int64_t queue_size() const;
    int64_t completed_tasks() const { return _completed_tasks.load(); }
    int64_t failed_tasks() const { return _failed_tasks.load(); }
    int64_t recovered_tablets() const { return _recovered_tablets.load(); }

    // Check if a tablet has pending compaction results
    bool has_pending_results(int64_t tablet_id) const;

    // Get the set of rowsets currently being compacted for a tablet
    std::unordered_set<uint32_t> get_compacting_rowsets(int64_t tablet_id) const;

    // ============== Partial Compaction Support (Mechanism 4) ==============
    
    // Check if a tablet has partial compaction state (some results exist, others running)
    bool has_partial_compaction_state(int64_t tablet_id) const;

private:
    // Background thread function that processes the priority queue
    void _schedule_thread_func();

    // Try to schedule a compaction task for a tablet
    Status _schedule_tablet(int64_t tablet_id);

    // Execute a compaction task
    void _execute_compaction(std::unique_ptr<CompactionTaskContext> context,
                             std::vector<uint32_t> input_rowset_ids);

    // Calculate compaction score for a tablet
    StatusOr<double> _calculate_score(int64_t tablet_id);

    // Update tablet state after score calculation
    void _update_tablet_state(int64_t tablet_id, double score);

    // Check if we can schedule more tasks for a tablet
    bool _can_schedule_tablet(const TabletCompactionState& state);

    // Mark rowsets as being compacted. Caller must hold _state_mutex.
    void _mark_rowsets_compacting(int64_t tablet_id, const std::vector<uint32_t>& rowset_ids);

    // Unmark rowsets after compaction completes. Caller must hold _state_mutex.
    void _unmark_rowsets_compacting(int64_t tablet_id, const std::vector<uint32_t>& rowset_ids);

    TabletManager* _tablet_mgr;
    std::unique_ptr<CompactionResultManager> _result_mgr;

    // Priority queue for tablets pending compaction
    std::priority_queue<TabletCompactionInfo> _tablet_queue;
    mutable std::mutex _queue_mutex;
    std::condition_variable _queue_cv;

    // Per-tablet compaction state
    std::unordered_map<int64_t, TabletCompactionState> _tablet_states;
    mutable std::mutex _state_mutex;

    // Thread pool for executing compaction tasks
    std::unique_ptr<ThreadPool> _thread_pool;

    // Background scheduler thread
    std::unique_ptr<std::thread> _schedule_thread;

    // Metrics
    std::atomic<int64_t> _running_tasks{0};
    std::atomic<int64_t> _completed_tasks{0};
    std::atomic<int64_t> _failed_tasks{0};
    std::atomic<int64_t> _recovered_tablets{0};

    // Control flags
    std::atomic<bool> _stopped{false};
    std::atomic<bool> _recovered{false};
};

} // namespace starrocks::lake


