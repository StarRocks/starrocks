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
#include "glog/logging.h"
#include "runtime/exec_env.h"

namespace starrocks::workgroup {

namespace {

const char* sched_entity_type_to_string(ScanSchedEntityType sched_entity_type) {
    switch (sched_entity_type) {
    case ScanSchedEntityType::OLAP:
        return "OLAP";
    case ScanSchedEntityType::CONNECTOR:
        return "CONNECTOR";
    }
    return "UNKNOWN";
}

} // namespace

LockFreeWorkGroupScanTaskQueue::LockFreeWorkGroupScanTaskQueue(ScanSchedEntityType sched_entity_type, int num_workers)
        : _sched_entity_type(sched_entity_type), _num_workers(num_workers) {
    LOG(INFO) << "[SCAN_QUEUE] create LockFreeWorkGroupScanTaskQueue"
              << " sched_entity_type=" << sched_entity_type_to_string(_sched_entity_type)
              << " num_workers=" << _num_workers;
}

bool LockFreeWorkGroupScanTaskQueue::try_offer(ScanTask task) {
    if (task.peak_scan_task_queue_size_counter != nullptr) {
        task.peak_scan_task_queue_size_counter->set(_num_tasks.load(std::memory_order_relaxed));
    }
    WorkGroup* wg = task.workgroup.get();
    _set_wg_in_queue(task);
    auto* wg_queue = _get_or_create_wg_queue(wg);
    wg_queue->queue->try_offer(std::move(task));
    if (wg_queue->num_tasks.fetch_add(1, std::memory_order_relaxed) == 0) {
        _rebase_vruntime_on_first_enqueue(wg);
    }
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _update_min_wg_entity_on_enqueue(wg);
    _sema.signal();
    return true;
}

void LockFreeWorkGroupScanTaskQueue::force_put(ScanTask task) {
    if (task.peak_scan_task_queue_size_counter != nullptr) {
        task.peak_scan_task_queue_size_counter->set(_num_tasks.load(std::memory_order_relaxed));
    }
    WorkGroup* wg = task.workgroup.get();
    _set_wg_in_queue(task);
    auto* wg_queue = _get_or_create_wg_queue(wg);
    wg_queue->queue->force_put(std::move(task));
    if (wg_queue->num_tasks.fetch_add(1, std::memory_order_relaxed) == 0) {
        _rebase_vruntime_on_first_enqueue(wg);
    }
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _update_min_wg_entity_on_enqueue(wg);
    _sema.signal();
}

StatusOr<ScanTask> LockFreeWorkGroupScanTaskQueue::take() {
    return take(-1);
}

StatusOr<ScanTask> LockFreeWorkGroupScanTaskQueue::take(int worker_id) {
    const bool use_token = (worker_id >= 0 && worker_id < _num_workers);
    while (true) {
        if (_closed.load(std::memory_order_acquire)) {
            return Status::Cancelled("Shutdown");
        }

        // Try ALL workgroups in ascending vruntime order before blocking.
        auto candidates = _pick_sorted_wgs();
        bool skipped_due_to_yield = false;
        for (auto& [vruntime, wg_queue] : candidates) {
            if (ExecEnv::GetInstance()->workgroup_manager()->should_yield(wg_queue->workgroup)) {
                skipped_due_to_yield = true;
                continue;
            }
            ScanTask task;
            bool got = use_token ? wg_queue->queue->try_take(task, worker_id) : wg_queue->queue->try_take(task);
            if (got) {
                if (wg_queue->num_tasks.fetch_sub(1, std::memory_order_relaxed) == 1) {
                    _refresh_min_wg_entity();
                }
                _num_tasks.fetch_sub(1, std::memory_order_relaxed);
                // Enqueue signals _sema on every task arrival. Drain one permit on
                // the fast dequeue path as well, otherwise sustained traffic can
                // accumulate stale permits and keep workers spinning after the queue
                // goes idle.
                _sema.tryWait();
                return std::move(task);
            }
        }

        if (skipped_due_to_yield) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
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

LockFreeWorkGroupScanTaskQueue::WorkGroupQueueState* LockFreeWorkGroupScanTaskQueue::_get_or_create_wg_queue(
        WorkGroup* wg) {
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
    auto [inserted_it, ok] = _wg_queues.emplace(wg, std::make_unique<WorkGroupQueueState>(wg, _num_workers));
    LOG(INFO) << "[SCAN_QUEUE] create per-workgroup LockFreeScanTaskQueue"
              << " sched_entity_type=" << sched_entity_type_to_string(_sched_entity_type) << " wg_id=" << wg->id()
              << " wg_name=" << wg->name() << " num_workers=" << _num_workers;
    return inserted_it->second.get();
}

void LockFreeWorkGroupScanTaskQueue::_update_min_wg_entity_on_enqueue(WorkGroup* wg) {
    const auto* entity = _sched_entity(wg);
    const auto* min_entity = _min_wg_entity.load(std::memory_order_relaxed);
    while (min_entity == nullptr || entity->vruntime_ns() < min_entity->vruntime_ns()) {
        if (_min_wg_entity.compare_exchange_weak(min_entity, entity, std::memory_order_relaxed)) {
            return;
        }
    }
}

void LockFreeWorkGroupScanTaskQueue::_refresh_min_wg_entity() {
    const WorkGroupScanSchedEntity* min_entity = nullptr;
    int64_t min_vrt = std::numeric_limits<int64_t>::max();

    std::shared_lock read_lock(_wg_queues_mutex);
    for (auto& [wg, queue_state] : _wg_queues) {
        if (queue_state->num_tasks.load(std::memory_order_relaxed) == 0) {
            continue;
        }
        auto* entity = _sched_entity(wg);
        if (entity->vruntime_ns() < min_vrt) {
            min_vrt = entity->vruntime_ns();
            min_entity = entity;
        }
    }
    _min_wg_entity.store(min_entity, std::memory_order_relaxed);
}

void LockFreeWorkGroupScanTaskQueue::_rebase_vruntime_on_first_enqueue(WorkGroup* wg) {
    auto* wg_entity = const_cast<WorkGroupScanSchedEntity*>(_sched_entity(wg));
    const WorkGroupScanSchedEntity* min_entity = nullptr;
    int64_t min_vruntime_ns = std::numeric_limits<int64_t>::max();
    size_t num_runnable_wgs = 0;
    int64_t sum_cpu_weight = 0;

    std::shared_lock read_lock(_wg_queues_mutex);
    for (auto& [other_wg, queue_state] : _wg_queues) {
        if (other_wg == wg || queue_state->num_tasks.load(std::memory_order_relaxed) == 0) {
            continue;
        }
        auto* entity = _sched_entity(other_wg);
        ++num_runnable_wgs;
        sum_cpu_weight += entity->cpu_weight();
        if (entity->vruntime_ns() < min_vruntime_ns) {
            min_vruntime_ns = entity->vruntime_ns();
            min_entity = entity;
        }
    }

    if (min_entity == nullptr || sum_cpu_weight == 0) {
        return;
    }

    int64_t ideal_runtime_ns = SCHEDULE_PERIOD_PER_WG_NS * num_runnable_wgs * wg_entity->cpu_weight() / sum_cpu_weight;
    int64_t new_vruntime_ns = std::min(min_entity->vruntime_ns() - ideal_runtime_ns / 2,
                                       min_entity->runtime_ns() / int64_t(wg_entity->cpu_weight()));
    int64_t diff_vruntime_ns = new_vruntime_ns - wg_entity->vruntime_ns();
    if (diff_vruntime_ns > 0) {
        wg_entity->adjust_runtime_ns(diff_vruntime_ns * wg_entity->cpu_weight());
    }
}

LockFreeWorkGroupScanTaskQueue::CandidateList LockFreeWorkGroupScanTaskQueue::_pick_sorted_wgs() {
    CandidateList candidates;

    // Snapshot all workgroup queues under one shared lock acquisition.
    std::vector<WorkGroupQueueState*> snapshot;
    {
        std::shared_lock read_lock(_wg_queues_mutex);
        snapshot.reserve(_wg_queues.size());
        for (auto& [wg, queue_state] : _wg_queues) {
            if (queue_state->num_tasks.load(std::memory_order_relaxed) > 0) {
                snapshot.emplace_back(queue_state.get());
            }
        }
    }

    // Score by vruntime without holding any lock.
    candidates.reserve(snapshot.size());
    for (auto* queue_state : snapshot) {
        int64_t vrt = _sched_entity(queue_state->workgroup)->vruntime_ns();
        candidates.emplace_back(vrt, queue_state);
    }

    std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

    // Update min entity cache for should_yield().
    if (!candidates.empty()) {
        // Find the workgroup with minimum vruntime from snapshot.
        const WorkGroupScanSchedEntity* min_entity = nullptr;
        int64_t min_vrt = std::numeric_limits<int64_t>::max();
        for (auto* queue_state : snapshot) {
            auto* entity = _sched_entity(queue_state->workgroup);
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
