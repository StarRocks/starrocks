// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/pipeline_driver_queue.h"

#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group.h"
#include "gutil/strings/substitute.h"

namespace starrocks::pipeline {

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
        _cv.notify_one();
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
        _cv.notify_one();
    }
}

void QuerySharedDriverQueue::put_back_from_executor(const DriverRawPtr driver) {
    // QuerySharedDriverQueue::put_back_from_executor is identical to put_back.
    put_back(driver);
}

void QuerySharedDriverQueue::put_back_from_executor(const std::vector<DriverRawPtr>& drivers) {
    // QuerySharedDriverQueue::put_back_from_executor is identical to put_back.
    put_back(drivers);
}

StatusOr<DriverRawPtr> QuerySharedDriverQueue::take(int worker_id) {
    // -1 means no candidates; else has candidate.
    int queue_idx = -1;
    double target_accu_time = 0;
    DriverRawPtr driver_ptr;

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
            _cv.wait(lock);
        }
        // record queue's index to accumulate time for it.
        driver_ptr = _queues[queue_idx].take();
        driver_ptr->set_in_ready_queue(false);
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

    size_t size = 0;
    for (const auto& sub_queue : _queues) {
        size += sub_queue.size();
    }
    return size;
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

QuerySharedDriverQueueWithoutLock::QuerySharedDriverQueueWithoutLock() {
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

void QuerySharedDriverQueueWithoutLock::put_back(const DriverRawPtr driver) {
    _put_back(driver);
}

void QuerySharedDriverQueueWithoutLock::put_back(const std::vector<DriverRawPtr>& drivers) {
    for (auto driver : drivers) {
        _put_back(driver);
    }
}

void QuerySharedDriverQueueWithoutLock::put_back_from_executor(const DriverRawPtr driver) {
    // QuerySharedDriverQueueWithoutLock::put_back_from_executor is identical to put_back.
    put_back(driver);
}

void QuerySharedDriverQueueWithoutLock::put_back_from_executor(const std::vector<DriverRawPtr>& drivers) {
    // QuerySharedDriverQueueWithoutLock::put_back_from_executor is identical to put_back.
    put_back(drivers);
}

StatusOr<DriverRawPtr> QuerySharedDriverQueueWithoutLock::take(int worker_id) {
    // -1 means no candidates; else has candidate.
    int queue_idx = -1;
    double target_accu_time = 0;
    DriverRawPtr driver_ptr;

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

    // Always return non-null driver, which is guaranteed be the callee, e.g. DriverQueueWithWorkGroup::take().
    DCHECK(queue_idx >= 0);
    driver_ptr = _queues[queue_idx].take();
    driver_ptr->set_in_ready_queue(false);

    --_size;

    return driver_ptr;
}

void QuerySharedDriverQueueWithoutLock::cancel(DriverRawPtr driver) {
    if (!driver->is_in_ready_queue()) {
        return;
    }
    int level = driver->get_driver_queue_level();
    _queues[level].cancel(driver);
}

void QuerySharedDriverQueueWithoutLock::update_statistics(const DriverRawPtr driver) {
    _queues[driver->get_driver_queue_level()].update_accu_time(driver);
}

void QuerySharedDriverQueueWithoutLock::_put_back(const DriverRawPtr driver) {
    int level = _compute_driver_level(driver);
    driver->set_driver_queue_level(level);
    _queues[level].put(driver);
    ++_size;
}

int QuerySharedDriverQueueWithoutLock::_compute_driver_level(const DriverRawPtr driver) const {
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
    driver_number++;
}

void SubQuerySharedDriverQueue::cancel(const DriverRawPtr driver) {
    if (cancelled_set.count(driver) == 0) {
        DCHECK(driver->is_in_ready_queue());
        pending_cancel_queue.emplace(driver);
    }
}

DriverRawPtr SubQuerySharedDriverQueue::take() {
    DCHECK(!empty());
    if (!pending_cancel_queue.empty()) {
        DriverRawPtr driver = pending_cancel_queue.front();
        pending_cancel_queue.pop();
        cancelled_set.insert(driver);
        --driver_number;
        return driver;
    }

    while (!queue.empty()) {
        DriverRawPtr driver = queue.front();
        queue.pop_front();
        auto iter = cancelled_set.find(driver);
        if (iter != cancelled_set.end()) {
            cancelled_set.erase(iter);
        } else {
            --driver_number;
            return driver;
        }
    }
    return nullptr;
}

void DriverQueueWithWorkGroup::close() {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _is_closed = true;
    _cv.notify_all();
}

void DriverQueueWithWorkGroup::put_back(const DriverRawPtr driver) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _put_back<false>(driver);
}

void DriverQueueWithWorkGroup::put_back(const std::vector<DriverRawPtr>& drivers) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    for (const auto driver : drivers) {
        _put_back<false>(driver);
    }
}

void DriverQueueWithWorkGroup::put_back_from_executor(const DriverRawPtr driver) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _put_back<true>(driver);
}

void DriverQueueWithWorkGroup::put_back_from_executor(const std::vector<DriverRawPtr>& drivers) {
    std::lock_guard<std::mutex> lock(_global_mutex);

    for (const auto driver : drivers) {
        _put_back<true>(driver);
    }
}

StatusOr<DriverRawPtr> DriverQueueWithWorkGroup::take(int worker_id) {
    std::unique_lock<std::mutex> lock(_global_mutex);

    if (_is_closed) {
        return Status::Cancelled("Shutdown");
    }

    while (_ready_wgs.empty()) {
        _cv.wait(lock);
        if (_is_closed) {
            return Status::Cancelled("Shutdown");
        }
    }

    // Try to take driver from any owner workgroup first.
    auto wg = _find_min_owner_wg(worker_id);
    if (wg == nullptr) {
        // All the owner workgroups don't have ready drivers, so select the other workgroup.
        wg = _find_min_wg();
    }
    DCHECK(wg != nullptr);

    // If wg only contains one ready driver, it will be not ready anymore after taking away
    // the only one driver.
    if (wg->driver_queue()->size() == 1) {
        _sum_cpu_limit -= wg->cpu_limit();
        _ready_wgs.erase(wg);
    }

    return wg->driver_queue()->take(worker_id);
}

void DriverQueueWithWorkGroup::cancel(DriverRawPtr driver) {
    std::unique_lock<std::mutex> lock(_global_mutex);
    if (_is_closed) {
        return;
    }
    if (!driver->is_in_ready_queue()) {
        return;
    }
    auto* wg = driver->workgroup();
    wg->driver_queue()->cancel(driver);
}

void DriverQueueWithWorkGroup::update_statistics(const DriverRawPtr driver) {
    // TODO: reduce the lock scope
    std::unique_lock<std::mutex> lock(_global_mutex);

    int64_t runtime_ns = driver->driver_acct().get_last_time_spent();
    auto* wg = driver->workgroup();
    wg->driver_queue()->update_statistics(driver);
    wg->increment_real_runtime_ns(runtime_ns);
    workgroup::WorkGroupManager::instance()->increment_cpu_runtime_ns(runtime_ns);

    // For big query check cpu
    if (wg->big_query_cpu_second_limit()) {
        // Calculate the cost of the source && sink operator alone
        int64_t source_operator_last_cpu_time_ns = driver->source_operator()->get_last_growth_cpu_time_ns();
        int64_t sink_operator_last_cpu_time_ns = driver->sink_operator()->get_last_growth_cpu_time_ns();
        int64_t accounted_cpu_cost = runtime_ns + source_operator_last_cpu_time_ns + sink_operator_last_cpu_time_ns;

        wg->incr_total_cpu_cost(accounted_cpu_cost);
    }
}

size_t DriverQueueWithWorkGroup::size() const {
    // TODO: reduce the lock scope
    std::unique_lock<std::mutex> lock(_global_mutex);

    size_t size = 0;
    for (auto wg : _ready_wgs) {
        size += wg->driver_queue()->size();
    }

    return size;
}

template <bool from_executor>
void DriverQueueWithWorkGroup::_put_back(const DriverRawPtr driver) {
    auto* wg = driver->workgroup();
    if (_ready_wgs.find(wg) == _ready_wgs.end()) {
        _sum_cpu_limit += wg->cpu_limit();
        // The runtime needn't be adjusted for the workgroup put back from executor thread,
        // because it has updated before executor thread put the workgroup back by update_statistics().
        if constexpr (!from_executor) {
            auto* min_wg = _find_min_wg();
            if (min_wg != nullptr) {
                int64_t origin_real_runtime_ns = wg->real_runtime_ns();

                // The workgroup maybe leaves for a long time, which results in that the runtime of it
                // may be much smaller than the other workgroups. If the runtime isn't adjusted, the others
                // will starve. Therefore, the runtime is adjusted according the minimum vruntime in _ready_wgs,
                // and give it half of ideal runtime in a schedule period as compensation.
                int64_t new_vruntime_ns = std::min(min_wg->vruntime_ns() - _ideal_runtime_ns(wg) / 2,
                                                   min_wg->real_runtime_ns() / int64_t(wg->cpu_limit()));
                wg->set_vruntime_ns(std::max(wg->vruntime_ns(), new_vruntime_ns));
                wg->update_last_real_runtime_ns(wg->real_runtime_ns());

                int64_t diff_real_runtime_ns = wg->real_runtime_ns() - origin_real_runtime_ns;
                workgroup::WorkGroupManager::instance()->increment_cpu_runtime_ns(diff_real_runtime_ns);
                wg->incr_total_cpu_cost(diff_real_runtime_ns);
            }
        }
        _ready_wgs.emplace(wg);
    }
    wg->driver_queue()->put_back(driver);
    _cv.notify_one();
}

workgroup::WorkGroup* DriverQueueWithWorkGroup::_find_min_owner_wg(int worker_id) {
    workgroup::WorkGroup* min_wg = nullptr;
    int64_t min_vruntime_ns = 0;

    auto owner_wgs = workgroup::WorkGroupManager::instance()->get_owners_of_driver_worker(worker_id);
    if (owner_wgs != nullptr) {
        for (const auto& wg : *owner_wgs) {
            if (_ready_wgs.find(wg.get()) != _ready_wgs.end() &&
                (min_wg == nullptr || min_vruntime_ns > wg->vruntime_ns())) {
                min_wg = wg.get();
                min_vruntime_ns = wg->vruntime_ns();
            }
        }
    }

    return min_wg;
}

workgroup::WorkGroup* DriverQueueWithWorkGroup::_find_min_wg() {
    workgroup::WorkGroup* min_wg = nullptr;
    int64_t min_vruntime_ns = 0;

    for (auto wg : _ready_wgs) {
        if (min_wg == nullptr || min_vruntime_ns > wg->vruntime_ns()) {
            min_wg = wg;
            min_vruntime_ns = wg->vruntime_ns();
        }
    }
    return min_wg;
}

int64_t DriverQueueWithWorkGroup::_ideal_runtime_ns(workgroup::WorkGroup* wg) {
    return SCHEDULE_PERIOD_PER_WG_NS * _ready_wgs.size() * wg->cpu_limit() / _sum_cpu_limit;
}

} // namespace starrocks::pipeline
