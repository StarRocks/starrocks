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

#include "exec/pipeline/work_stealing_queue.h"
#include "exec/workgroup/scan_task_queue.h"

namespace starrocks::workgroup {

// LockFreeScanTaskQueue is a lock-free per-workgroup priority queue for scan
// tasks. It replaces PriorityScanTaskQueue by using WorkStealingQueue internally.
//
// Priority values 0-20 map 1:1 to 21 queue levels. Dequeue scans from level 20
// (highest priority) down to level 0 (lowest priority), returning the first
// non-empty level's task.
class LockFreeScanTaskQueue {
public:
    static constexpr int NUM_PRIORITY_LEVELS = 21;

    explicit LockFreeScanTaskQueue(int num_workers);

    // Non-copyable, non-movable.
    LockFreeScanTaskQueue(const LockFreeScanTaskQueue&) = delete;
    LockFreeScanTaskQueue& operator=(const LockFreeScanTaskQueue&) = delete;
    LockFreeScanTaskQueue(LockFreeScanTaskQueue&&) = delete;
    LockFreeScanTaskQueue& operator=(LockFreeScanTaskQueue&&) = delete;

    // Enqueue a task at its priority level. Worker thread variant uses
    // pre-allocated ProducerToken for reduced contention.
    // Always returns true since the underlying ConcurrentQueue allocates.
    bool try_offer(ScanTask task, int worker_id);
    // External thread variant uses implicit producer path.
    bool try_offer(ScanTask task);

    // Enqueue a task at its priority level. Semantically identical to try_offer
    // but provided for interface compatibility with existing queue types.
    void force_put(ScanTask task, int worker_id);
    void force_put(ScanTask task);

    // Dequeue a task. Scans from level 20 (highest priority) down to level 0.
    // Returns the first task found. Returns false if all levels are empty.
    bool try_take(ScanTask& task);

    // Returns the approximate total number of enqueued tasks across all levels.
    size_t size() const;

private:
    // Clamp a priority value to the valid range [0, NUM_PRIORITY_LEVELS - 1].
    static int _clamp_priority(int priority);

    pipeline::WorkStealingQueue<ScanTask, NUM_PRIORITY_LEVELS> _queue;
};

} // namespace starrocks::workgroup
