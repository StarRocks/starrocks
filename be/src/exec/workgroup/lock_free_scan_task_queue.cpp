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

#include <algorithm>

namespace starrocks::workgroup {

LockFreeScanTaskQueue::LockFreeScanTaskQueue(int num_workers) : _queue(num_workers) {}

bool LockFreeScanTaskQueue::try_offer(ScanTask task, int worker_id) {
    int level = _clamp_priority(task.priority);
    _queue.enqueue(std::move(task), level, worker_id);
    return true;
}

bool LockFreeScanTaskQueue::try_offer(ScanTask task) {
    int level = _clamp_priority(task.priority);
    _queue.enqueue(std::move(task), level);
    return true;
}

void LockFreeScanTaskQueue::force_put(ScanTask task, int worker_id) {
    int level = _clamp_priority(task.priority);
    _queue.enqueue(std::move(task), level, worker_id);
}

void LockFreeScanTaskQueue::force_put(ScanTask task) {
    int level = _clamp_priority(task.priority);
    _queue.enqueue(std::move(task), level);
}

bool LockFreeScanTaskQueue::try_take(ScanTask& task) {
    // Scan from highest priority (level 20) down to lowest (level 0).
    for (int level = NUM_PRIORITY_LEVELS - 1; level >= 0; --level) {
        if (_queue.try_dequeue(level, task)) {
            return true;
        }
    }
    return false;
}

size_t LockFreeScanTaskQueue::size() const {
    return _queue.size_approx();
}

int LockFreeScanTaskQueue::_clamp_priority(int priority) {
    return std::clamp(priority, 0, NUM_PRIORITY_LEVELS - 1);
}

} // namespace starrocks::workgroup
