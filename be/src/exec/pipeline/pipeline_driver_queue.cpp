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

#include "exec/pipeline/pipeline_driver_queue.h"

#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group.h"
#include "gutil/strings/substitute.h"

namespace starrocks::pipeline {

/// QuerySharedDriverQueue.
QuerySharedDriverQueue::QuerySharedDriverQueue() {
    double factor = 1;
    for (int i = QUEUE_SIZE - 1; i >= 0; --i) {
        // initialize factor for every sub queue,
        // Higher priority queues have more execution time,
        // so they have a larger factor.
        _queues[i].factor_for_normal = factor;
        factor *= RATIO_OF_ADJACENT_QUEUE;
    }

    int64_t time_slice = 0;
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        time_slice += LEVEL_TIME_SLICE_BASE_NS * (i + 1);
        _level_time_slices[i] = time_slice;
    }
}

void QuerySharedDriverQueue::close() {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _is_closed = true;
    _cv.notify_all();
}

void QuerySharedDriverQueue::put_back(const DriverRawPtr driver) {
    int level = _compute_driver_level(driver);
    driver->set_driver_queue_level(level);
    {
        std::lock_guard<std::mutex> lock(_global_mutex);
        _queues[level].put(driver);
        driver->set_in_ready_queue(true);
        driver->set_in_queue(this);
        driver->update_peak_driver_queue_size_counter(_num_drivers);
        _cv.notify_one();
        ++_num_drivers;
    }
}

void QuerySharedDriverQueue::put_back(const std::vector<DriverRawPtr>& drivers) {
    std::vector<int> levels(drivers.size());
    for (int i = 0; i < drivers.size(); i++) {
        levels[i] = _compute_driver_level(drivers[i]);
        drivers[i]->set_driver_queue_level(levels[i]);
    }
    std::lock_guard<std::mutex> lock(_global_mutex);
    for (int i = 0; i < drivers.size(); i++) {
        _queues[levels[i]].put(drivers[i]);
        drivers[i]->set_in_ready_queue(true);
        drivers[i]->set_in_queue(this);
        drivers[i]->update_peak_driver_queue_size_counter(_num_drivers);
        _cv.notify_one();
    }
    _num_drivers += drivers.size();
}

void QuerySharedDriverQueue::put_back_from_executor(const DriverRawPtr driver) {
    // QuerySharedDriverQueue::put_back_from_executor is identical to put_back.
    put_back(driver);
}

StatusOr<DriverRawPtr> QuerySharedDriverQueue::take(const bool block) {
    // -1 means no candidates; else has candidate.
    int queue_idx = -1;
    double target_accu_time = 0;
    DriverRawPtr driver_ptr = nullptr;

    {
        std::unique_lock<std::mutex> lock(_global_mutex);
        while (true) {
            if (_is_closed) {
                return Status::Cancelled("Shutdown");
            }

            // Find the queue with the smallest execution time.
            for (int i = 0; i < QUEUE_SIZE; ++i) {
                // we just search for queue has element
                if (!_queues[i].empty()) {
                    double local_target_time = _queues[i].accu_time_after_divisor();
                    if (queue_idx < 0 || local_target_time < target_accu_time) {
                        target_accu_time = local_target_time;
                        queue_idx = i;
                    }
                }
            }

            if (queue_idx >= 0) {
                break;
            }
            if (!block) {
                break;
            }
            _cv.wait(lock);
        }

        if (queue_idx >= 0) {
            // record queue's index to accumulate time for it.
            driver_ptr = _queues[queue_idx].take(false);
            driver_ptr->set_in_ready_queue(false);

            --_num_drivers;
        }
    }

    // next pipeline driver to execute.
    return driver_ptr;
}

void QuerySharedDriverQueue::cancel(DriverRawPtr driver) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    if (_is_closed) {
        return;
    }
    if (!driver->is_in_ready_queue()) {
        return;
    }
    int level = driver->get_driver_queue_level();
    _queues[level].cancel(driver);
    _cv.notify_one();
}

size_t QuerySharedDriverQueue::size() const {
    std::lock_guard<std::mutex> lock(_global_mutex);

    return _num_drivers;
}

void QuerySharedDriverQueue::update_statistics(const DriverRawPtr driver) {
    std::lock_guard<std::mutex> lock(_global_mutex);

    _queues[driver->get_driver_queue_level()].update_accu_time(driver);
}

int QuerySharedDriverQueue::_compute_driver_level(const DriverRawPtr driver) const {
    int time_spent = driver->driver_acct().get_accumulated_time_spent();
    for (int i = driver->get_driver_queue_level(); i < QUEUE_SIZE; ++i) {
        if (time_spent < _level_time_slices[i]) {
            return i;
        }
    }

    return QUEUE_SIZE - 1;
}

void SubQuerySharedDriverQueue::put(const DriverRawPtr driver) {
    if (driver->driver_state() == DriverState::CANCELED) {
        queue.emplace_front(driver);
    } else {
        queue.emplace_back(driver);
    }
    num_drivers++;
}

void SubQuerySharedDriverQueue::cancel(const DriverRawPtr driver) {
    if (cancelled_set.count(driver) == 0) {
        DCHECK(driver->is_in_ready_queue());
        pending_cancel_queue.emplace(driver);
    }
}

DriverRawPtr SubQuerySharedDriverQueue::take(const bool block) {
    DCHECK(!empty());
    DCHECK(!block);
    if (!pending_cancel_queue.empty()) {
        DriverRawPtr driver = pending_cancel_queue.front();
        pending_cancel_queue.pop();
        cancelled_set.insert(driver);
        --num_drivers;
        return driver;
    }

    while (!queue.empty()) {
        DriverRawPtr driver = queue.front();
        queue.pop_front();
        auto iter = cancelled_set.find(driver);
        if (iter != cancelled_set.end()) {
            cancelled_set.erase(iter);
        } else {
            --num_drivers;
            return driver;
        }
    }
    return nullptr;
}

/// WorkGroupDriverQueue.
bool WorkGroupDriverQueue::WorkGroupDriverSchedEntityComparator::operator()(
        const WorkGroupDriverSchedEntityPtr& lhs_ptr, const WorkGroupDriverSchedEntityPtr& rhs_ptr) const {
    int64_t lhs_val = lhs_ptr->vruntime_ns();
    int64_t rhs_val = rhs_ptr->vruntime_ns();
    if (lhs_val != rhs_val) {
        return lhs_val < rhs_val;
    }
    return lhs_ptr < rhs_ptr;
}

void WorkGroupDriverQueue::close() {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _is_closed = true;
    _cv.notify_all();
}

void WorkGroupDriverQueue::put_back(const DriverRawPtr driver) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _put_back<false>(driver);
}

void WorkGroupDriverQueue::put_back(const std::vector<DriverRawPtr>& drivers) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    for (const auto driver : drivers) {
        _put_back<false>(driver);
    }
}

void WorkGroupDriverQueue::put_back_from_executor(const DriverRawPtr driver) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _put_back<true>(driver);
}

StatusOr<DriverRawPtr> WorkGroupDriverQueue::take(const bool block) {
    std::unique_lock<std::mutex> lock(_global_mutex);

    workgroup::WorkGroupDriverSchedEntity* wg_entity = nullptr;
    while (wg_entity == nullptr) {
        if (_is_closed) {
            return Status::Cancelled("Shutdown");
        }

        _update_bandwidth_control_period();

        if (_wg_entities.empty()) {
            if (!block) {
                return nullptr;
            }
            _cv.wait(lock);
        } else if (wg_entity = _take_next_wg(); wg_entity == nullptr) {
            int64_t cur_ns = MonotonicNanos();
            int64_t sleep_ns = _bandwidth_control_period_end_ns - cur_ns;
            if (sleep_ns <= 0) {
                continue;
            }

            if (!block) {
                return nullptr;
            }
            // All the ready tasks are throttled, so wait until the new period or a new task comes.
            _cv.wait_for(lock, std::chrono::nanoseconds(sleep_ns));
        }
    }

    // If wg only contains one ready driver, it will be not ready anymore
    // after taking away the only one driver.
    if (wg_entity->queue()->size() == 1) {
        _dequeue_workgroup(wg_entity);
    }

    auto maybe_driver = wg_entity->queue()->take(block);
    if (maybe_driver.ok() && maybe_driver.value() != nullptr) {
        --_num_drivers;
    }
    return maybe_driver;
}

void WorkGroupDriverQueue::cancel(DriverRawPtr driver) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    if (_is_closed) {
        return;
    }
    if (!driver->is_in_ready_queue()) {
        return;
    }
    auto* wg_entity = driver->workgroup()->driver_sched_entity();
    wg_entity->queue()->cancel(driver);
}

void WorkGroupDriverQueue::update_statistics(const DriverRawPtr driver) {
    // TODO: reduce the lock scope
    std::lock_guard<std::mutex> lock(_global_mutex);

    int64_t runtime_ns = driver->driver_acct().get_last_time_spent();
    auto* wg_entity = driver->workgroup()->driver_sched_entity();

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
    wg_entity->incr_runtime_ns(runtime_ns);
    if (is_in_queue) {
        _wg_entities.emplace(wg_entity);
        _update_min_wg();
    }

    wg_entity->queue()->update_statistics(driver);
}

size_t WorkGroupDriverQueue::size() const {
    // TODO: reduce the lock scope
    std::lock_guard<std::mutex> lock(_global_mutex);
    return _num_drivers;
}

bool WorkGroupDriverQueue::should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const {
    if (_throttled(driver->workgroup()->driver_sched_entity(), unaccounted_runtime_ns)) {
        return true;
    }

    // Return true, if the minimum-vruntime workgroup is not current workgroup anymore.
    auto* wg_entity = driver->workgroup()->driver_sched_entity();
    auto* min_entity = _min_wg_entity.load();
    return min_entity != wg_entity && min_entity &&
           min_entity->vruntime_ns() < wg_entity->vruntime_ns() + unaccounted_runtime_ns / wg_entity->cpu_limit();
}

bool WorkGroupDriverQueue::_throttled(const workgroup::WorkGroupDriverSchedEntity* wg_entity,
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

template <bool from_executor>
void WorkGroupDriverQueue::_put_back(const DriverRawPtr driver) {
    driver->update_peak_driver_queue_size_counter(_num_drivers);

    auto* wg_entity = driver->workgroup()->driver_sched_entity();
    wg_entity->set_in_queue(this);
    wg_entity->queue()->put_back(driver);
    driver->set_in_queue(this);

    if (_wg_entities.find(wg_entity) == _wg_entities.end()) {
        _enqueue_workgroup<from_executor>(wg_entity);
    }

    ++_num_drivers;

    _cv.notify_one();
}

void WorkGroupDriverQueue::_update_min_wg() {
    auto* min_wg_entity = _take_next_wg();
    if (min_wg_entity == nullptr) {
        _min_wg_entity = nullptr;
    } else {
        _min_wg_entity = min_wg_entity;
    }
}

workgroup::WorkGroupDriverSchedEntity* WorkGroupDriverQueue::_take_next_wg() {
    workgroup::WorkGroupDriverSchedEntity* min_unthrottled_wg_entity = nullptr;
    for (auto* wg_entity : _wg_entities) {
        if (!_throttled(wg_entity)) {
            min_unthrottled_wg_entity = wg_entity;
            break;
        }
    }

    return min_unthrottled_wg_entity;
}

template <bool from_executor>
void WorkGroupDriverQueue::_enqueue_workgroup(workgroup::WorkGroupDriverSchedEntity* wg_entity) {
    _sum_cpu_limit += wg_entity->cpu_limit();
    // The runtime needn't be adjusted for the workgroup put back from executor thread,
    // because it has updated before executor thread put the workgroup back by update_statistics().
    if constexpr (!from_executor) {
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
    }

    _wg_entities.emplace(wg_entity);
    _update_min_wg();
}

void WorkGroupDriverQueue::_dequeue_workgroup(workgroup::WorkGroupDriverSchedEntity* wg_entity) {
    _sum_cpu_limit -= wg_entity->cpu_limit();
    _wg_entities.erase(wg_entity);
    _update_min_wg();
}

int64_t WorkGroupDriverQueue::_ideal_runtime_ns(workgroup::WorkGroupDriverSchedEntity* wg_entity) const {
    return SCHEDULE_PERIOD_PER_WG_NS * _wg_entities.size() * wg_entity->cpu_limit() / _sum_cpu_limit;
}

void WorkGroupDriverQueue::_update_bandwidth_control_period() {
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

int64_t WorkGroupDriverQueue::_bandwidth_quota_ns() const {
    return BANDWIDTH_CONTROL_PERIOD_NS * workgroup::WorkGroupManager::instance()->normal_workgroup_cpu_hard_limit();
}

} // namespace starrocks::pipeline
