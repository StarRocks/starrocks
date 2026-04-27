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
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"

namespace starrocks::lake {

class TabletManager;
class CompactionResultManager;

// Event-driven autonomous compaction scheduler (Phase 4).
//
// Producer: callers invoke update_tablet_async(tablet_id) when something might
// have changed compaction priority (e.g. publish_version succeeded, a previous
// autonomous task finished, manual trigger).
//
// Consumer: a single dispatch thread pops the highest-score tablet, computes
// excluded rowsets (running_inputs ∪ pending_inputs from CompactionResultManager),
// and submits a compaction task into the existing CompactionScheduler thread pool.
//
// Per-tablet concurrency is capped by lake_autonomous_compaction_max_tasks_per_tablet.
// Global concurrency is capped by lake_autonomous_compaction_max_concurrent_tasks.
//
// This class is intentionally light: it only orchestrates. The actual compaction
// execution still goes through the existing per-task path (compaction_scheduler.cpp
// + compaction_task.cpp), which we wire to write CompactionResultPB locally via
// CompactionTaskContext::write_to_local_result.
class LakeCompactionManager {
public:
    static LakeCompactionManager* instance();

    // Lifecycle. start() launches the dispatch thread; stop() joins it.
    void start(TabletManager* tablet_mgr, CompactionResultManager* result_mgr);
    void stop();

    // Producer entry point. Score-evaluates the tablet asynchronously and
    // enqueues it (or refreshes its priority) if eligible. Idempotent and
    // safe to call from any thread.
    void update_tablet_async(int64_t tablet_id);

    // Notify on task lifecycle so we can decrement counters and trigger
    // self-continuation when a task finishes.
    void notify_task_finished(int64_t tablet_id, const std::vector<uint32_t>& consumed_input_rowsets);

    // Accessor for the BE-global CompactionResultManager that this manager
    // controls. Returns nullptr until start() runs. Used by compaction_scheduler
    // and other lake-internal callers to avoid touching ExecEnv::GetInstance.
    CompactionResultManager* result_manager() const { return _result_mgr; }

    // Test/debug accessors.
    size_t queue_size() const;
    int64_t running_tasks() const { return _running_total.load(std::memory_order_relaxed); }
    int64_t running_tasks_for_tablet(int64_t tablet_id) const;
    std::unordered_set<uint32_t> running_inputs(int64_t tablet_id) const;

private:
    LakeCompactionManager() = default;
    ~LakeCompactionManager() = default;
    LakeCompactionManager(const LakeCompactionManager&) = delete;
    LakeCompactionManager& operator=(const LakeCompactionManager&) = delete;

    struct TabletEntry {
        int64_t tablet_id = 0;
        double score = 0.0;
        int64_t enqueue_time_ms = 0;

        // Min-heap inversion: bigger score floats to top.
        bool operator<(const TabletEntry& other) const {
            if (score != other.score) return score < other.score;
            return enqueue_time_ms > other.enqueue_time_ms;
        }
    };

    void dispatch_loop();

    // Compute compaction score for a tablet. Returns negative on error.
    double compute_score_locked(int64_t tablet_id);

    // Try to schedule one task; returns true if one was dispatched.
    bool try_dispatch_one_locked();

    bool _started = false;
    std::atomic<bool> _stopping{false};
    std::thread _dispatch_thread;

    TabletManager* _tablet_mgr = nullptr;
    CompactionResultManager* _result_mgr = nullptr;

    mutable std::mutex _mu;
    std::condition_variable _cv;

    std::priority_queue<TabletEntry> _queue;
    // Membership index so we can dedupe duplicate update_tablet_async calls.
    std::unordered_set<int64_t> _enqueued;

    // Per-tablet running count; used to enforce max_tasks_per_tablet and to
    // skip tablets that are already saturated.
    std::unordered_map<int64_t, int64_t> _running_per_tablet;
    // Per-tablet "running_inputs": rowset_ids currently being compacted by
    // tasks dispatched but not yet finished. Combined with CompactionResultManager's
    // pending_inputs to form the excluded set for pick_rowsets_with_limit.
    std::unordered_map<int64_t, std::unordered_multiset<uint32_t>> _running_inputs;
    std::atomic<int64_t> _running_total{0};
};

} // namespace starrocks::lake
