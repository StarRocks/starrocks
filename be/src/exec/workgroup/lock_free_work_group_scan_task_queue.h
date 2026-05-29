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
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "base/concurrency/moodycamel/blockingconcurrentqueue.h"
#include "exec/workgroup/lock_free_scan_task_queue.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

/// LockFreeWorkGroupScanTaskQueue implements the ScanTaskQueue interface using
/// lock-free per-workgroup queues. Same pattern as LockFreeWorkGroupDriverQueue
/// but adapted for ScanTask.
/// No cancel mechanism — scan tasks handle cancellation at execution level.
class LockFreeWorkGroupScanTaskQueue : public ScanTaskQueue {
public:
    LockFreeWorkGroupScanTaskQueue(ScanSchedEntityType sched_entity_type, int num_workers);
    ~LockFreeWorkGroupScanTaskQueue() override = default;

    LockFreeWorkGroupScanTaskQueue(const LockFreeWorkGroupScanTaskQueue&) = delete;
    LockFreeWorkGroupScanTaskQueue& operator=(const LockFreeWorkGroupScanTaskQueue&) = delete;

    void close() override;

    StatusOr<ScanTask> take() override;
    StatusOr<ScanTask> take(int worker_id) override;
    bool try_offer(ScanTask task) override;
    void force_put(ScanTask task) override;

    size_t size() const override;

    void update_statistics(ScanTask& task, int64_t runtime_ns) override;
    bool should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const override;

#ifdef BE_TEST
    size_t available_wakeup_permits_for_test() const { return _sema.availableApprox(); }
#endif

private:
    static constexpr int64_t SCHEDULE_PERIOD_PER_WG_NS = 100'000'000;

    struct WorkGroupQueueState {
        explicit WorkGroupQueueState(WorkGroup* workgroup, int num_workers)
                : workgroup(workgroup), queue(std::make_unique<LockFreeScanTaskQueue>(num_workers)) {}

        WorkGroup* workgroup;
        std::unique_ptr<LockFreeScanTaskQueue> queue;
        std::atomic<size_t> num_tasks{0};
    };

    using CandidateList = std::vector<std::pair<int64_t, WorkGroupQueueState*>>;

    void _set_wg_in_queue(const ScanTask& task);
    void _update_min_wg_entity_on_enqueue(WorkGroup* wg);
    void _refresh_min_wg_entity();
    void _rebase_vruntime_on_first_enqueue(WorkGroup* wg);

    WorkGroupQueueState* _get_or_create_wg_queue(WorkGroup* wg);
    CandidateList _pick_sorted_wgs();

    const WorkGroupScanSchedEntity* _sched_entity(const WorkGroup* wg) const;

    ScanSchedEntityType _sched_entity_type;
    int _num_workers;
    std::atomic<size_t> _num_tasks{0};
    std::atomic<bool> _closed{false};

    // Cached min-vruntime entity for lock-free should_yield check.
    std::atomic<const WorkGroupScanSchedEntity*> _min_wg_entity{nullptr};

    moodycamel::LightweightSemaphore _sema;

    mutable std::shared_mutex _wg_queues_mutex;
    std::unordered_map<WorkGroup*, std::unique_ptr<WorkGroupQueueState>> _wg_queues;
};

} // namespace starrocks::workgroup
