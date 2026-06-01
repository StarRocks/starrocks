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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <set>

#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

class WorkGroupScanTaskQueue final : public ScanTaskQueue {
public:
    WorkGroupScanTaskQueue(ScanSchedEntityType sched_entity_type) : _sched_entity_type(sched_entity_type) {}
    ~WorkGroupScanTaskQueue() override = default;

    void close() override;

    StatusOr<ScanTask> take() override;
    bool try_offer(ScanTask task) override;
    void force_put(ScanTask task) override;

    size_t size() const override { return _num_tasks.load(std::memory_order_acquire); }

    void update_statistics(ScanTask& task, int64_t runtime_ns) override;
    bool should_yield(const WorkGroupScanSchedEntity* scan_sched_entity, int64_t unaccounted_runtime_ns) const override;

private:
    /// These methods should be guarded by the outside _global_mutex.
    WorkGroupScanSchedEntity* _pick_next_wg() const;
    // _update_min_wg is invoked when an entity is enqueued or dequeued from _wg_entities.
    void _update_min_wg();
    void _enqueue_workgroup(WorkGroupScanSchedEntity* wg_entity);
    void _dequeue_workgroup(WorkGroupScanSchedEntity* wg_entity);

    // The ideal runtime of a work group is the weighted average of the schedule period.
    int64_t _ideal_runtime_ns(WorkGroupScanSchedEntity* wg_entity) const;

    WorkGroupScanSchedEntity* _sched_entity(WorkGroup* wg);
    const WorkGroupScanSchedEntity* _sched_entity(const WorkGroup* wg) const;

private:
    static constexpr int64_t SCHEDULE_PERIOD_PER_WG_NS = 100'000'000;

    struct WorkGroupScanSchedEntityComparator {
        using WorkGroupScanSchedEntityPtr = WorkGroupScanSchedEntity*;
        bool operator()(const WorkGroupScanSchedEntityPtr& lhs_ptr, const WorkGroupScanSchedEntityPtr& rhs_ptr) const;
    };
    using WorkgroupSet = std::set<WorkGroupScanSchedEntity*, WorkGroupScanSchedEntityComparator>;

    const ScanSchedEntityType _sched_entity_type;

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    std::condition_variable _cv_for_borrowed_cpus;
    bool _is_closed = false;

    // Contains the workgroups which include the tasks ready to be run.
    // Entities are sorted by vruntime in set.
    // MUST guarantee the entity is not in set, when updating its vruntime.
    WorkgroupSet _wg_entities;

    size_t _sum_cpu_weight = 0;

    // Cache the minimum entity, used to check should_yield() without lock.
    std::atomic<WorkGroupScanSchedEntity*> _min_wg_entity = nullptr;

    std::atomic<size_t> _num_tasks = 0;
};

} // namespace starrocks::workgroup
