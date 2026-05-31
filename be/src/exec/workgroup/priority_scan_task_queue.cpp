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

#include "exec/workgroup/priority_scan_task_queue.h"

#include <utility>

#include "common/status.h"

namespace starrocks::workgroup {

PriorityScanTaskQueue::PriorityScanTaskQueue(size_t max_elements) : _queue(max_elements) {}

StatusOr<ScanTask> PriorityScanTaskQueue::take() {
    ScanTask task;
    if (_queue.blocking_get(&task)) {
        return task;
    }

    return Status::Cancelled("Shutdown");
}

bool PriorityScanTaskQueue::try_take(ScanTask* task) {
    return _queue.non_blocking_get(task);
}

bool PriorityScanTaskQueue::try_offer(ScanTask task) {
    if (task.peak_scan_task_queue_size_counter != nullptr) {
        task.peak_scan_task_queue_size_counter->set(_queue.get_size());
    }
    return _queue.try_put(std::move(task));
}

void PriorityScanTaskQueue::force_put(ScanTask task) {
    if (task.peak_scan_task_queue_size_counter != nullptr) {
        task.peak_scan_task_queue_size_counter->set(_queue.get_size());
    }
    _queue.force_put(std::move(task));
}

} // namespace starrocks::workgroup
