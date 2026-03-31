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

#include "base/concurrency/moodycamel/lightweightsemaphore.h"
#include "exec/workgroup/lock_free_scan_task_queue.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

// LockFreeWorkGroupScanTaskQueue is the cross-workgroup composition layer that
// manages multiple LockFreeScanTaskQueue instances (one per workgroup). It replaces
// WorkGroupScanTaskQueue with a lock-free design using:
//   - Atomic vruntime scanning for workgroup selection
//   - moodycamel::LightweightSemaphore for blocking
//
// Each workgroup gets its own LockFreeScanTaskQueue that is lazily created and
// stored in an internal map. Workgroup selection picks the workgroup with the
// minimum vruntime among those with queued tasks.
//
// Unlike the driver queue counterpart, scan tasks handle cancellation at
// execution level, so no cancel mechanism is provided here.
class LockFreeWorkGroupScanTaskQueue {
public:
    explicit LockFreeWorkGroupScanTaskQueue(int num_workers);
    ~LockFreeWorkGroupScanTaskQueue() = default;

    // Non-copyable, non-movable.
    LockFreeWorkGroupScanTaskQueue(const LockFreeWorkGroupScanTaskQueue&) = delete;
    LockFreeWorkGroupScanTaskQueue& operator=(const LockFreeWorkGroupScanTaskQueue&) = delete;
    LockFreeWorkGroupScanTaskQueue(LockFreeWorkGroupScanTaskQueue&&) = delete;
    LockFreeWorkGroupScanTaskQueue& operator=(LockFreeWorkGroupScanTaskQueue&&) = delete;

    // Enqueue a task. Worker thread variant uses worker_id for reduced contention.
    // Always returns true since the underlying ConcurrentQueue allocates.
    bool try_offer(ScanTask task, int worker_id);
    // External thread variant uses implicit producer path.
    bool try_offer(ScanTask task);

    // Enqueue a task. Semantically identical to try_offer but provided for
    // interface compatibility with existing queue types.
    void force_put(ScanTask task, int worker_id);
    void force_put(ScanTask task);

    // Dequeue a task. Selects the workgroup with minimum vruntime among those
    // with queued tasks, then dequeues from that workgroup's LockFreeScanTaskQueue.
    // If blocking=true, waits on the semaphore until a task is available or
    // the queue is closed.
    // Returns true if a task was obtained; false if non-blocking and empty, or closed.
    bool take(ScanTask& task, bool blocking);

    // Update workgroup's scan sched entity vruntime after a scan task
    // completes an execution quantum.
    void update_statistics(ScanTask& task, int64_t runtime_ns);

    // Close the queue and wake all blocked consumers.
    void close();

    // Returns the approximate total number of enqueued tasks across all workgroups.
    size_t size() const;

private:
    // Get or create the LockFreeScanTaskQueue for a workgroup.
    LockFreeScanTaskQueue* _get_or_create_wg_queue(WorkGroup* wg);

    // Scan active workgroups for the one with minimum vruntime that has queued tasks.
    // Returns nullptr if no workgroup has queued tasks.
    WorkGroupScanSchedEntity* _pick_next_wg();

    int _num_workers;
    std::atomic<size_t> _num_tasks{0};
    std::atomic<bool> _closed{false};

    moodycamel::LightweightSemaphore _sema;

    // Per-workgroup LockFreeScanTaskQueue instances. Protected by _wg_queues_mutex.
    mutable std::mutex _wg_queues_mutex;
    std::unordered_map<WorkGroup*, std::unique_ptr<LockFreeScanTaskQueue>> _wg_queues;
};

} // namespace starrocks::workgroup
