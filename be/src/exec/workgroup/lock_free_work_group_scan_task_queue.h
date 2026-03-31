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

#include <algorithm>
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "base/concurrency/moodycamel/lightweightsemaphore.h"
#include "exec/workgroup/lock_free_scan_task_queue.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

/// LockFreeWorkGroupScanTaskQueue is the cross-workgroup composition layer for scan tasks.
/// Same pattern as LockFreeWorkGroupDriverQueue but adapted for ScanTask.
/// No cancel mechanism — scan tasks handle cancellation at execution level.
class LockFreeWorkGroupScanTaskQueue {
public:
    explicit LockFreeWorkGroupScanTaskQueue(int num_workers);
    ~LockFreeWorkGroupScanTaskQueue() = default;

    LockFreeWorkGroupScanTaskQueue(const LockFreeWorkGroupScanTaskQueue&) = delete;
    LockFreeWorkGroupScanTaskQueue& operator=(const LockFreeWorkGroupScanTaskQueue&) = delete;

    bool try_offer(ScanTask task, int worker_id);
    bool try_offer(ScanTask task);
    void force_put(ScanTask task, int worker_id);
    void force_put(ScanTask task);

    /// Tries ALL workgroups in ascending vruntime order before blocking.
    bool take(ScanTask& task, bool blocking);

    void update_statistics(ScanTask& task, int64_t runtime_ns);
    void close();
    size_t size() const;

private:
    using CandidateList = std::vector<std::pair<int64_t, LockFreeScanTaskQueue*>>;

    LockFreeScanTaskQueue* _get_or_create_wg_queue(WorkGroup* wg);
    CandidateList _pick_sorted_wgs();

    int _num_workers;
    std::atomic<size_t> _num_tasks{0};
    std::atomic<bool> _closed{false};

    moodycamel::LightweightSemaphore _sema;

    mutable std::shared_mutex _wg_queues_mutex;
    std::unordered_map<WorkGroup*, std::unique_ptr<LockFreeScanTaskQueue>> _wg_queues;
};

} // namespace starrocks::workgroup
