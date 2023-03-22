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

/// MultiLevelFeedScanTaskQueue.
MultiLevelFeedScanTaskQueue::MultiLevelFeedScanTaskQueue() {
    double factor = 1;
    for (int i = NUM_QUEUES - 1; i >= 0; --i) {
        _queues[i].factor_for_normal = factor;
        factor *= RATIO_OF_ADJACENT_QUEUE;
    }

    int64_t time_slice = 0;
    for (int i = 0; i < NUM_QUEUES; ++i) {
        time_slice += LEVEL_TIME_SLICE_BASE_NS * (i + 1);
        _queues[i].level_time_slice = time_slice;
    }
}

void MultiLevelFeedScanTaskQueue::close() {
    std::lock_guard<std::mutex> lock(_global_mutex);

    if (_is_closed) {
        return;
    }

    _is_closed = true;
    _cv.notify_all();
}

StatusOr<ScanTask> MultiLevelFeedScanTaskQueue::take() {
    std::unique_lock<std::mutex> lock(_global_mutex);

    int queue_idx = -1;
    double target_accu_time = 0;

    while (true) {
        if (_is_closed) {
            return Status::Cancelled("Shutdown");
        }

        // Find the queue with the smallest execution time.
        for (int i = 0; i < NUM_QUEUES; ++i) {
            // we just search for queue has element
            if (!_queues[i].queue.empty()) {
                double local_target_time = _queues[i].normalized_cost();
                if (queue_idx < 0 || local_target_time < target_accu_time) {
                    target_accu_time = local_target_time;
                    queue_idx = i;
                }
            }
        }

        if (queue_idx >= 0) {
            break;
        }

        _cv.wait(lock);
    }

    auto& queue = _queues[queue_idx].queue;
    auto task = std::move(queue.front());
    queue.pop();
    _num_tasks--;
    return task;
}

bool MultiLevelFeedScanTaskQueue::try_offer(ScanTask task) {
    std::lock_guard<std::mutex> lock(_global_mutex);

    if (task.peak_scan_task_queue_size_counter != nullptr) {
        task.peak_scan_task_queue_size_counter->set(_num_tasks);
    }

    int level = _compute_queue_level(task);
    task.task_group->sub_queue_level = level;
    _queues[level].queue.push(std::move(task));
    _num_tasks++;

    _cv.notify_one();
    return true;
}

void MultiLevelFeedScanTaskQueue::update_statistics(ScanTask& task, int64_t runtime_ns) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    task.task_group->runtime_ns += runtime_ns;
    _queues[task.task_group->sub_queue_level].incr_cost_ns(runtime_ns);
}

int MultiLevelFeedScanTaskQueue::_compute_queue_level(const ScanTask& task) const {
    int64_t time_spent = task.task_group->runtime_ns;
    for (int i = task.task_group->sub_queue_level; i < NUM_QUEUES; ++i) {
        if (time_spent < _queues[i].level_time_slice) {
            return i;
        }
    }

    return NUM_QUEUES - 1;
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
}

StatusOr<ScanTask> WorkGroupScanTaskQueue::take() {
    std::unique_lock<std::mutex> lock(_global_mutex);

    workgroup::WorkGroupScanSchedEntity* wg_entity = nullptr;
    while (wg_entity == nullptr) {
        if (_is_closed) {
            return Status::Cancelled("Shutdown");
        }

        _update_bandwidth_control_period();

        if (_wg_entities.empty()) {
            _cv.wait(lock);
        } else if (wg_entity = _take_next_wg(); wg_entity == nullptr) {
            int64_t cur_ns = MonotonicNanos();
            int64_t sleep_ns = _bandwidth_control_period_end_ns - cur_ns;
            if (sleep_ns <= 0) {
                continue;
            }

            // All the ready tasks are throttled, so wait until the new period or a new task comes.
            _cv.wait_for(lock, std::chrono::nanoseconds(sleep_ns));
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

void WorkGroupScanTaskQueue::update_statistics(ScanTask& task, int64_t runtime_ns) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    auto* wg = task.workgroup;
    auto* wg_entity = _sched_entity(wg);

    // Update bandwidth control information.
    _update_bandwidth_control_period();
    if (!wg_entity->is_sq_wg()) {
        _bandwidth_usage_ns += runtime_ns;
    }

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
    if (_throttled(_sched_entity(wg), unaccounted_runtime_ns)) {
        return true;
    }

    // Return true, if the minimum-vruntime workgroup is not current workgroup anymore.
    auto* wg_entity = _sched_entity(wg);
    auto* min_entity = _min_wg_entity.load();
    return min_entity != wg_entity && min_entity &&
           min_entity->vruntime_ns() < wg_entity->vruntime_ns() + unaccounted_runtime_ns / wg_entity->cpu_limit();
}

bool WorkGroupScanTaskQueue::_throttled(const workgroup::WorkGroupScanSchedEntity* wg_entity,
                                        int64_t unaccounted_runtime_ns) const {
    if (wg_entity->is_sq_wg()) {
        return false;
    }
    if (!workgroup::WorkGroupManager::instance()->is_sq_wg_running()) {
        return false;
    }

    int64_t bandwidth_usage = unaccounted_runtime_ns + _bandwidth_usage_ns;
    return bandwidth_usage >= _bandwidth_quota_ns();
}

void WorkGroupScanTaskQueue::_update_min_wg() {
    auto* min_wg_entity = _take_next_wg();
    if (min_wg_entity == nullptr) {
        _min_wg_entity = nullptr;
    } else {
        _min_wg_entity = min_wg_entity;
    }
}

workgroup::WorkGroupScanSchedEntity* WorkGroupScanTaskQueue::_take_next_wg() {
    workgroup::WorkGroupScanSchedEntity* min_unthrottled_wg_entity = nullptr;
    for (const auto& wg_entity : _wg_entities) {
        if (!_throttled(wg_entity)) {
            min_unthrottled_wg_entity = wg_entity;
            break;
        }
    }

    return min_unthrottled_wg_entity;
}

void WorkGroupScanTaskQueue::_enqueue_workgroup(workgroup::WorkGroupScanSchedEntity* wg_entity) {
    _sum_cpu_limit += wg_entity->cpu_limit();

    if (auto* min_wg_entity = _min_wg_entity.load(); min_wg_entity != nullptr) {
        // The workgroup maybe leaves for a long time, which results in that the runtime of it
        // may be much smaller than the other workgroups. If the runtime isn't adjusted, the others
        // will starve. Therefore, the runtime is adjusted according the minimum vruntime in _ready_wgs,
        // and give it half of ideal runtime in a schedule period as compensation.
        int64_t new_vruntime_ns = std::min(min_wg_entity->vruntime_ns() - _ideal_runtime_ns(wg_entity) / 2,
                                           min_wg_entity->runtime_ns() / int64_t(wg_entity->cpu_limit()));
        int64_t diff_vruntime_ns = new_vruntime_ns - wg_entity->vruntime_ns();
        if (diff_vruntime_ns > 0) {
            DCHECK(_wg_entities.find(wg_entity) == _wg_entities.end());
            wg_entity->adjust_runtime_ns(diff_vruntime_ns * wg_entity->cpu_limit());
        }
    }

    _wg_entities.emplace(wg_entity);
    _update_min_wg();
}

void WorkGroupScanTaskQueue::_dequeue_workgroup(workgroup::WorkGroupScanSchedEntity* wg_entity) {
    _sum_cpu_limit -= wg_entity->cpu_limit();
    _wg_entities.erase(wg_entity);
    _update_min_wg();
}

int64_t WorkGroupScanTaskQueue::_ideal_runtime_ns(workgroup::WorkGroupScanSchedEntity* wg_entity) const {
    return SCHEDULE_PERIOD_PER_WG_NS * _wg_entities.size() * wg_entity->cpu_limit() / _sum_cpu_limit;
}

void WorkGroupScanTaskQueue::_update_bandwidth_control_period() {
    int64_t cur_ns = MonotonicNanos();
    if (_bandwidth_control_period_end_ns == 0 || _bandwidth_control_period_end_ns <= cur_ns) {
        _bandwidth_control_period_end_ns = cur_ns + BANDWIDTH_CONTROL_PERIOD_NS;

        int64_t bandwidth_quota = _bandwidth_quota_ns();
        int64_t bandwidth_usage = _bandwidth_usage_ns.load();
        if (bandwidth_usage <= bandwidth_quota) {
            _bandwidth_usage_ns = 0;
        } else if (bandwidth_usage < 2 * bandwidth_quota) {
            _bandwidth_usage_ns -= bandwidth_quota;
        } else {
            _bandwidth_usage_ns = bandwidth_quota;
        }
    }
}

int64_t WorkGroupScanTaskQueue::_bandwidth_quota_ns() const {
    return BANDWIDTH_CONTROL_PERIOD_NS * workgroup::WorkGroupManager::instance()->normal_workgroup_cpu_hard_limit();
}

workgroup::WorkGroupScanSchedEntity* WorkGroupScanTaskQueue::_sched_entity(workgroup::WorkGroup* wg) {
    if (_sched_entity_type == SchedEntityType::CONNECTOR) {
        return wg->connector_scan_sched_entity();
    } else {
        return wg->scan_sched_entity();
    }
}

const workgroup::WorkGroupScanSchedEntity* WorkGroupScanTaskQueue::_sched_entity(const workgroup::WorkGroup* wg) const {
    if (_sched_entity_type == SchedEntityType::CONNECTOR) {
        return wg->connector_scan_sched_entity();
    } else {
        return wg->scan_sched_entity();
    }
}

std::unique_ptr<ScanTaskQueue> create_scan_task_queue() {
    switch (config::pipeline_scan_queue_mode) {
    case 0:
        return std::make_unique<PriorityScanTaskQueue>(config::pipeline_scan_thread_pool_queue_size);
    case 1:
        return std::make_unique<MultiLevelFeedScanTaskQueue>();
    default:
        return std::make_unique<PriorityScanTaskQueue>(config::pipeline_scan_thread_pool_queue_size);
    }
}

} // namespace starrocks::workgroup
