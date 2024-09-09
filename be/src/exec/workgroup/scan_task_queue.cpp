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

#include "exec/workgroup/scan_task_queue.h"

#include "common/status.h"
#include "exec/workgroup/work_group.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

/// PriorityScanTaskQueue.
PriorityScanTaskQueue::PriorityScanTaskQueue(size_t max_elements) : _queue(max_elements) {}

StatusOr<ScanTask> PriorityScanTaskQueue::take() {
    ScanTask task;
    if (_queue.blocking_get(&task)) {
        return task;
    }

    return Status::Cancelled("Shutdown");
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

/// WorkGroupScanTaskQueue.
bool WorkGroupScanTaskQueue::WorkGroupScanSchedEntityComparator::operator()(
        const WorkGroupScanSchedEntityPtr& lhs_ptr, const WorkGroupScanSchedEntityPtr& rhs_ptr) const {
    int64_t lhs_val = lhs_ptr->vruntime_ns();
    int64_t rhs_val = rhs_ptr->vruntime_ns();
    if (lhs_val != rhs_val) {
        return lhs_val < rhs_val;
    }
    return lhs_ptr < rhs_ptr;
}

void WorkGroupScanTaskQueue::close() {
    std::lock_guard<std::mutex> lock(_global_mutex);

    if (_is_closed) {
        return;
    }

    _is_closed = true;
    _cv.notify_all();
    _cv_for_borrowed_cpus.notify_all();
}

StatusOr<ScanTask> WorkGroupScanTaskQueue::take() {
    std::unique_lock<std::mutex> lock(_global_mutex);

    WorkGroupScanSchedEntity* wg_entity = nullptr;
    while (true) {
        if (_is_closed) {
            return Status::Cancelled("Shutdown");
        }

        // For task queue used by exclusive workgroup, driver queue always contains only tasks of this workgroup,
        // so `_pick_next_wg` will always return this workgroup.
        // TODO: In the future, we may implement different task queues for exclusive workgroup and shared workgroup,
        // since exclusive workgroup does not need two-level queues about workgroup.
        wg_entity = _pick_next_wg();
        if (wg_entity != nullptr &&
            !ExecEnv::GetInstance()->workgroup_manager()->should_yield(wg_entity->workgroup())) {
            break;
        }

        if (wg_entity == nullptr) {
            _cv.wait(lock);
        } else {
            // This thread can only run on the borrowed CPU. At this time, the owner of the borrowed CPU has a task
            // coming, so give up the CPU.
            // And wake up the threads running on its own CPU to continue processing the task.
            _cv.notify_one();
            _cv_for_borrowed_cpus.wait_for(lock, std::chrono::milliseconds(50));
        }
    }

    // If wg only contains one ready task, it will be not ready anymore
    // after taking away the only one task.
    if (wg_entity->queue()->size() == 1) {
        _dequeue_workgroup(wg_entity);
    }

    _num_tasks--;

    return wg_entity->queue()->take();
}

bool WorkGroupScanTaskQueue::try_offer(ScanTask task) {
    std::lock_guard<std::mutex> lock(_global_mutex);

    if (task.peak_scan_task_queue_size_counter != nullptr) {
        task.peak_scan_task_queue_size_counter->set(_num_tasks);
    }

    auto* wg_entity = _sched_entity(task.workgroup);
    wg_entity->set_in_queue(this);
    RETURN_IF_UNLIKELY(!wg_entity->queue()->try_offer(std::move(task)), false);

    if (_wg_entities.find(wg_entity) == _wg_entities.end()) {
        _enqueue_workgroup(wg_entity);
    }

    _num_tasks++;
    _cv.notify_one();
    return true;
}

void WorkGroupScanTaskQueue::force_put(ScanTask task) {
    std::lock_guard<std::mutex> lock(_global_mutex);

    if (task.peak_scan_task_queue_size_counter != nullptr) {
        task.peak_scan_task_queue_size_counter->set(_num_tasks);
    }

    auto* wg_entity = _sched_entity(task.workgroup);
    wg_entity->set_in_queue(this);
    wg_entity->queue()->force_put(std::move(task));

    if (_wg_entities.find(wg_entity) == _wg_entities.end()) {
        _enqueue_workgroup(wg_entity);
    }

    _num_tasks++;
    _cv.notify_one();
}

void WorkGroupScanTaskQueue::update_statistics(ScanTask& task, int64_t runtime_ns) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    auto* wg = task.workgroup;
    auto* wg_entity = _sched_entity(wg);

    // Update sched entity information.
    bool is_in_queue = _wg_entities.find(wg_entity) != _wg_entities.end();
    if (is_in_queue) {
        _wg_entities.erase(wg_entity);
    }
    DCHECK(_wg_entities.find(wg_entity) == _wg_entities.end());
    wg_entity->queue()->update_statistics(task, runtime_ns);
    wg_entity->incr_runtime_ns(runtime_ns);
    if (is_in_queue) {
        _wg_entities.emplace(wg_entity);
        _update_min_wg();
    }
}

bool WorkGroupScanTaskQueue::should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const {
    if (ExecEnv::GetInstance()->workgroup_manager()->should_yield(wg)) {
        return true;
    }

    // Return true, if the minimum-vruntime workgroup is not current workgroup anymore.
    const auto* wg_entity = _sched_entity(wg);
    const auto* min_entity = _min_wg_entity.load();
    return min_entity != wg_entity && min_entity &&
           min_entity->vruntime_ns() < wg_entity->vruntime_ns() + unaccounted_runtime_ns / wg_entity->cpu_weight();
}

void WorkGroupScanTaskQueue::_update_min_wg() {
    auto* min_wg_entity = _pick_next_wg();
    if (min_wg_entity == nullptr) {
        _min_wg_entity = nullptr;
    } else {
        _min_wg_entity = min_wg_entity;
    }
}

WorkGroupScanSchedEntity* WorkGroupScanTaskQueue::_pick_next_wg() const {
    if (_wg_entities.empty()) {
        return nullptr;
    }

    return *_wg_entities.begin();
}

void WorkGroupScanTaskQueue::_enqueue_workgroup(WorkGroupScanSchedEntity* wg_entity) {
    _sum_cpu_weight += wg_entity->cpu_weight();

    if (auto* min_wg_entity = _min_wg_entity.load(); min_wg_entity != nullptr) {
        // The workgroup maybe leaves for a long time, which results in that the runtime of it
        // may be much smaller than the other workgroups. If the runtime isn't adjusted, the others
        // will starve. Therefore, the runtime is adjusted according the minimum vruntime in _ready_wgs,
        // and give it half of ideal runtime in a schedule period as compensation.
        int64_t new_vruntime_ns = std::min(min_wg_entity->vruntime_ns() - _ideal_runtime_ns(wg_entity) / 2,
                                           min_wg_entity->runtime_ns() / int64_t(wg_entity->cpu_weight()));
        int64_t diff_vruntime_ns = new_vruntime_ns - wg_entity->vruntime_ns();
        if (diff_vruntime_ns > 0) {
            DCHECK(_wg_entities.find(wg_entity) == _wg_entities.end());
            wg_entity->adjust_runtime_ns(diff_vruntime_ns * wg_entity->cpu_weight());
        }
    }

    _wg_entities.emplace(wg_entity);
    _update_min_wg();
}

void WorkGroupScanTaskQueue::_dequeue_workgroup(WorkGroupScanSchedEntity* wg_entity) {
    _sum_cpu_weight -= wg_entity->cpu_weight();
    _wg_entities.erase(wg_entity);
    _update_min_wg();
}

int64_t WorkGroupScanTaskQueue::_ideal_runtime_ns(WorkGroupScanSchedEntity* wg_entity) const {
    return SCHEDULE_PERIOD_PER_WG_NS * _wg_entities.size() * wg_entity->cpu_weight() / _sum_cpu_weight;
}

WorkGroupScanSchedEntity* WorkGroupScanTaskQueue::_sched_entity(WorkGroup* wg) {
    return const_cast<WorkGroupScanSchedEntity*>(_sched_entity(const_cast<const WorkGroup*>(wg)));
}

const WorkGroupScanSchedEntity* WorkGroupScanTaskQueue::_sched_entity(const WorkGroup* wg) const {
    if (_sched_entity_type == ScanSchedEntityType::CONNECTOR) {
        return wg->connector_scan_sched_entity();
    } else {
        return wg->scan_sched_entity();
    }
}

std::unique_ptr<ScanTaskQueue> create_scan_task_queue() {
    return std::make_unique<PriorityScanTaskQueue>(config::pipeline_scan_thread_pool_queue_size);
}

} // namespace starrocks::workgroup
