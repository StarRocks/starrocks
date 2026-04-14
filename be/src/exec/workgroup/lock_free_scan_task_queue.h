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

#include "exec/pipeline/multi_level_concurrent_queue.h"
#include "exec/workgroup/scan_task_queue.h"

namespace starrocks::workgroup {

// LockFreeScanTaskQueue is a lock-free per-workgroup priority queue for scan
// tasks. It replaces PriorityScanTaskQueue by using MultiLevelConcurrentQueue internally.
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

    // Enqueue a task at its priority level. All paths use the implicit producer.
    // The worker_id overload validates range via DCHECK.
    // Always returns true since the underlying ConcurrentQueue allocates.
    bool try_offer(ScanTask task, int worker_id);
    bool try_offer(ScanTask task);

    // Enqueue a task at its priority level. Semantically identical to try_offer
    // but provided for interface compatibility with existing queue types.
    void force_put(ScanTask task, int worker_id);
    void force_put(ScanTask task);

    // Dequeue a task. Scans from level 20 (highest priority) down to level 0.
    // Worker thread variant uses ConsumerToken for even load distribution.
    bool try_take(ScanTask& task, int worker_id);
    // Without worker_id (external or fallback).
    bool try_take(ScanTask& task);

    // Returns the approximate total number of enqueued tasks across all levels.
    size_t size() const;

private:
    void _mark_non_empty(int level);

    // Scan from highest priority down to lowest, trying dequeue at each non-empty level.
    // DequeueFunc: (int level, ScanTask& task) -> bool
    template <typename DequeueFunc>
    bool _try_take_from_levels(uint32_t bitmap, ScanTask& task, DequeueFunc&& dequeue);

    // Fallback: scan all levels directly, ignoring bitmap.
    // Used when bitmap-guided dequeue fails to guard against stale bitmap bits.
    template <typename DequeueFunc>
    bool _fallback_try_take(ScanTask& task, DequeueFunc&& dequeue);

    pipeline::MultiLevelConcurrentQueue<ScanTask, NUM_PRIORITY_LEVELS> _queue;

    // Bitmap of levels that are known to be non-empty.
    // bit i = 1 means level i MAY have items (no false negatives).
    // bit i = 0 means level i is DEFINITELY empty.
    // Replaces 21 × O(P) try_dequeue probes with a single atomic read on the hot path.
    std::atomic<uint32_t> _non_empty_bitmap{0};
};

} // namespace starrocks::workgroup
