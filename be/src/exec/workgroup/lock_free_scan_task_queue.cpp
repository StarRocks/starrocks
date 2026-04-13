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

#include "exec/workgroup/lock_free_scan_task_queue.h"

namespace starrocks::workgroup {

LockFreeScanTaskQueue::LockFreeScanTaskQueue(int num_workers) : _queue(num_workers) {}

bool LockFreeScanTaskQueue::try_offer(ScanTask task, int worker_id) {
    DCHECK(task.priority >= 0 && task.priority < NUM_PRIORITY_LEVELS);
    int level = task.priority;
    _queue.enqueue(std::move(task), level, worker_id);
    _mark_non_empty(level);
    return true;
}

bool LockFreeScanTaskQueue::try_offer(ScanTask task) {
    DCHECK(task.priority >= 0 && task.priority < NUM_PRIORITY_LEVELS);
    int level = task.priority;
    _queue.enqueue(std::move(task), level);
    _mark_non_empty(level);
    return true;
}

void LockFreeScanTaskQueue::force_put(ScanTask task, int worker_id) {
    DCHECK(task.priority >= 0 && task.priority < NUM_PRIORITY_LEVELS);
    int level = task.priority;
    _queue.enqueue(std::move(task), level, worker_id);
    _mark_non_empty(level);
}

void LockFreeScanTaskQueue::force_put(ScanTask task) {
    DCHECK(task.priority >= 0 && task.priority < NUM_PRIORITY_LEVELS);
    int level = task.priority;
    _queue.enqueue(std::move(task), level);
    _mark_non_empty(level);
}

bool LockFreeScanTaskQueue::try_take(ScanTask& task, int worker_id) {
    uint32_t bitmap = _non_empty_bitmap.load(std::memory_order_acquire);
    if (bitmap != 0) {
        if (_try_take_from_levels(bitmap, task, [this, worker_id](int level, ScanTask& t) {
                return _queue.try_dequeue(level, t, worker_id);
            })) {
            return true;
        }
    }

    // Bitmap may be stale due to a concurrent clear racing with an enqueue's
    // _mark_non_empty. Fall back to scanning all levels directly.
    return _fallback_try_take(
            task, [this, worker_id](int level, ScanTask& t) { return _queue.try_dequeue(level, t, worker_id); });
}

bool LockFreeScanTaskQueue::try_take(ScanTask& task) {
    uint32_t bitmap = _non_empty_bitmap.load(std::memory_order_acquire);
    if (bitmap != 0) {
        if (_try_take_from_levels(bitmap, task,
                                  [this](int level, ScanTask& t) { return _queue.try_dequeue(level, t); })) {
            return true;
        }
    }

    return _fallback_try_take(task, [this](int level, ScanTask& t) { return _queue.try_dequeue(level, t); });
}

template <typename DequeueFunc>
bool LockFreeScanTaskQueue::_try_take_from_levels(uint32_t bitmap, ScanTask& task, DequeueFunc&& dequeue) {
    // Scan from highest priority (level 20) down to lowest (level 0).
    // Only try levels with bitmap bit set.
    uint32_t to_clear = 0;
    for (int level = NUM_PRIORITY_LEVELS - 1; level >= 0; --level) {
        uint32_t mask = 1u << level;
        if (!(bitmap & mask)) continue;
        if (dequeue(level, task)) {
            if (to_clear) {
                _non_empty_bitmap.fetch_and(~to_clear, std::memory_order_relaxed);
            }
            return true;
        }
        to_clear |= mask;
    }

    // All attempted levels were empty — batch clear.
    _non_empty_bitmap.fetch_and(~to_clear, std::memory_order_relaxed);
    return false;
}

template <typename DequeueFunc>
bool LockFreeScanTaskQueue::_fallback_try_take(ScanTask& task, DequeueFunc&& dequeue) {
    for (int level = NUM_PRIORITY_LEVELS - 1; level >= 0; --level) {
        if (dequeue(level, task)) {
            _mark_non_empty(level);
            return true;
        }
    }
    return false;
}

size_t LockFreeScanTaskQueue::size() const {
    return _queue.size_approx();
}

void LockFreeScanTaskQueue::_mark_non_empty(int level) {
    uint32_t mask = 1u << level;
    if ((_non_empty_bitmap.load(std::memory_order_relaxed) & mask) == 0) {
        _non_empty_bitmap.fetch_or(mask, std::memory_order_relaxed);
    }
}

} // namespace starrocks::workgroup
