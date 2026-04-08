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

#include "exec/workgroup/lock_free_work_group_scan_task_queue.h"

#include <limits>

#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::workgroup {

LockFreeWorkGroupScanTaskQueue::LockFreeWorkGroupScanTaskQueue(ScanSchedEntityType sched_entity_type, int num_workers)
        : _sched_entity_type(sched_entity_type), _num_workers(num_workers) {}

bool LockFreeWorkGroupScanTaskQueue::try_offer(ScanTask task) {
    _set_wg_in_queue(task);
    auto* wg_queue = _get_or_create_wg_queue(task.workgroup.get());
    wg_queue->try_offer(std::move(task));
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
    return true;
}

void LockFreeWorkGroupScanTaskQueue::force_put(ScanTask task) {
    _set_wg_in_queue(task);
    auto* wg_queue = _get_or_create_wg_queue(task.workgroup.get());
    wg_queue->force_put(std::move(task));
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

StatusOr<ScanTask> LockFreeWorkGroupScanTaskQueue::take() {
    while (true) {
        if (_closed.load(std::memory_order_acquire)) {
            return Status::Cancelled("Shutdown");
        }

        // Try ALL workgroups in ascending vruntime order before blocking.
        auto candidates = _pick_sorted_wgs();
        for (auto& [vruntime, wg_queue] : candidates) {
            ScanTask task;
            if (wg_queue->try_take(task)) {
                _num_tasks.fetch_sub(1, std::memory_order_relaxed);
                return std::move(task);
            }
        }

        _sema.wait();
    }
}

void LockFreeWorkGroupScanTaskQueue::update_statistics(ScanTask& task, int64_t runtime_ns) {
    auto* entity = const_cast<WorkGroupScanSchedEntity*>(_sched_entity(task.workgroup.get()));
    entity->incr_runtime_ns(runtime_ns);
}

bool LockFreeWorkGroupScanTaskQueue::should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const {
    if (ExecEnv::GetInstance()->workgroup_manager()->should_yield(wg)) {
        return true;
    }
    const auto* wg_entity = _sched_entity(wg);
    const auto* min_entity = _min_wg_entity.load();
    return min_entity != wg_entity && min_entity &&
           min_entity->vruntime_ns() < wg_entity->vruntime_ns() + unaccounted_runtime_ns / wg_entity->cpu_weight();
}

void LockFreeWorkGroupScanTaskQueue::close() {
    _closed.store(true, std::memory_order_release);
    _sema.signal(_num_workers);
}

size_t LockFreeWorkGroupScanTaskQueue::size() const {
    return _num_tasks.load(std::memory_order_relaxed);
}

LockFreeScanTaskQueue* LockFreeWorkGroupScanTaskQueue::_get_or_create_wg_queue(WorkGroup* wg) {
    {
        std::shared_lock read_lock(_wg_queues_mutex);
        auto it = _wg_queues.find(wg);
        if (it != _wg_queues.end()) {
            return it->second.get();
        }
    }
    std::unique_lock write_lock(_wg_queues_mutex);
    auto it = _wg_queues.find(wg);
    if (it != _wg_queues.end()) {
        return it->second.get();
    }
    auto [inserted_it, ok] = _wg_queues.emplace(wg, std::make_unique<LockFreeScanTaskQueue>(_num_workers));
    return inserted_it->second.get();
}

LockFreeWorkGroupScanTaskQueue::CandidateList LockFreeWorkGroupScanTaskQueue::_pick_sorted_wgs() {
    CandidateList candidates;

    // Snapshot all workgroup queues under one shared lock acquisition.
    std::vector<std::pair<WorkGroup*, LockFreeScanTaskQueue*>> snapshot;
    {
        std::shared_lock read_lock(_wg_queues_mutex);
        snapshot.reserve(_wg_queues.size());
        for (auto& [wg, queue] : _wg_queues) {
            if (queue->size() > 0) {
                snapshot.emplace_back(wg, queue.get());
            }
        }
    }

    // Score by vruntime without holding any lock.
    candidates.reserve(snapshot.size());
    for (auto& [wg, queue] : snapshot) {
        int64_t vrt = _sched_entity(wg)->vruntime_ns();
        candidates.emplace_back(vrt, queue);
    }

    std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

    // Update min entity cache for should_yield().
    if (!candidates.empty()) {
        // Find the workgroup with minimum vruntime from snapshot.
        const WorkGroupScanSchedEntity* min_entity = nullptr;
        int64_t min_vrt = std::numeric_limits<int64_t>::max();
        for (auto& [wg, queue] : snapshot) {
            auto* entity = _sched_entity(wg);
            if (entity->vruntime_ns() < min_vrt) {
                min_vrt = entity->vruntime_ns();
                min_entity = entity;
            }
        }
        _min_wg_entity.store(min_entity, std::memory_order_relaxed);
    } else {
        _min_wg_entity.store(nullptr, std::memory_order_relaxed);
    }

    return candidates;
}

const WorkGroupScanSchedEntity* LockFreeWorkGroupScanTaskQueue::_sched_entity(const WorkGroup* wg) const {
    if (_sched_entity_type == ScanSchedEntityType::CONNECTOR) {
        return wg->connector_scan_sched_entity();
    }
    return wg->scan_sched_entity();
}

void LockFreeWorkGroupScanTaskQueue::_set_wg_in_queue(const ScanTask& task) {
    if (task.workgroup) {
        auto* entity = const_cast<WorkGroupScanSchedEntity*>(_sched_entity(task.workgroup.get()));
        entity->set_in_queue(this);
    }
}

} // namespace starrocks::workgroup
