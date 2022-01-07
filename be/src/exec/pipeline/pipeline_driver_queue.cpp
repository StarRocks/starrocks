// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_driver_queue.h"

#include "exec/workgroup/work_group.h"
#include "gutil/strings/substitute.h"

namespace starrocks::pipeline {

void QuerySharedDriverQueue::put_back(const DriverRawPtr driver, bool from_dispatcher) {
    int level = driver->driver_acct().get_level();
    _queues[level % QUEUE_SIZE].queue.emplace(driver);
    ++_size;
}

void QuerySharedDriverQueue::put_back(const std::vector<DriverRawPtr>& drivers, bool from_dispatcher) {
    std::vector<int> levels(drivers.size());
    for (int i = 0; i < drivers.size(); i++) {
        levels[i] = drivers[i]->driver_acct().get_level();
    }

    for (int i = 0; i < drivers.size(); i++) {
        _queues[levels[i] % QUEUE_SIZE].queue.emplace(drivers[i]);
    }
    _size += drivers.size();
}

StatusOr<DriverRawPtr> QuerySharedDriverQueue::take() {
    // -1 means no candidates; else has candidate.
    int queue_idx = -1;
    double target_accu_time = 0;
    DriverRawPtr driver_ptr;

    for (int i = 0; i < QUEUE_SIZE; ++i) {
        // we just search for queue has element
        if (!_queues[i].queue.empty()) {
            double local_target_time = _queues[i].accu_time_after_divisor();
            // if this is first queue that has element, we select it;
            // else we choose queue that the execution time is less sufficient,
            // and record time.
            if (queue_idx < 0 || local_target_time < target_accu_time) {
                target_accu_time = local_target_time;
                queue_idx = i;
            }
        }
    }

    DCHECK(queue_idx >= 0);

    driver_ptr = _queues[queue_idx].queue.front();
    driver_ptr->set_dispatch_queue_index(queue_idx);
    _queues[queue_idx].queue.pop();

    --_size;

    return driver_ptr;
}

void QuerySharedDriverQueue::yield_driver(const DriverRawPtr driver) {
    _queues[driver->get_dispatch_queue_index()].update_accu_time(driver);
}

void DriverQueueWithWorkGroup::close() {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _is_closed = true;
    _cv.notify_all();
}

void DriverQueueWithWorkGroup::put_back(const DriverRawPtr driver, bool from_dispatcher) {
    std::lock_guard<std::mutex> lock(_global_mutex);
    _put_back(driver, from_dispatcher);
}

void DriverQueueWithWorkGroup::put_back(const std::vector<DriverRawPtr>& drivers, bool from_dispatcher) {
    std::lock_guard<std::mutex> lock(_global_mutex);

    for (const auto driver : drivers) {
        _put_back(driver, from_dispatcher);
    }
}

StatusOr<DriverRawPtr> DriverQueueWithWorkGroup::take() {
    std::unique_lock<std::mutex> lock(_global_mutex);

    if (_is_closed) {
        return Status::Cancelled("Shutdown");
    }

    while (_wgs.empty()) {
        _cv.wait(lock);
        if (_is_closed) {
            return Status::Cancelled("Shutdown");
        }
    }

    auto wg = _find_min_wg();
    if (wg->driver_queue()->size() == 1) {
        _sum_cpu_limit -= wg->get_cpu_limit();
        _wgs.erase(wg);
    }

    return wg->driver_queue()->take();
}

void DriverQueueWithWorkGroup::yield_driver(const DriverRawPtr driver) {
    std::unique_lock<std::mutex> lock(_global_mutex);

    auto* wg = driver->workgroup();
    wg->driver_queue()->yield_driver(driver);
    wg->increment_real_runtime(driver->driver_acct().get_last_time_spent());
}

size_t DriverQueueWithWorkGroup::size() {
    std::lock_guard<std::mutex> lock(_global_mutex);

    size_t size = 0;
    for (auto wg : _wgs) {
        size += wg->driver_queue()->size();
    }

    return size;
}

void DriverQueueWithWorkGroup::_put_back(const DriverRawPtr driver, bool from_dispatcher) {
    auto* wg = driver->workgroup();
    if (_wgs.find(wg) == _wgs.end()) {
        _sum_cpu_limit += wg->get_cpu_limit();
        if (!from_dispatcher) {
            auto* min_wg = _find_min_wg();
            if (min_wg != nullptr) {
                wg->set_vruntime_ns(
                        std::max(wg->get_vruntime_ns(), min_wg->get_vruntime_ns() - _ideal_runtime_ns(wg) / 2));
            }
        }

        _wgs.emplace(wg);
    }

    wg->driver_queue()->put_back(driver, from_dispatcher);
    _cv.notify_one();
}

workgroup::WorkGroup* DriverQueueWithWorkGroup::_find_min_wg() {
    workgroup::WorkGroup* min_wg = nullptr;
    int64_t min_vruntime_ns = 0;
    for (auto wg : _wgs) {
        if (min_wg == nullptr || min_vruntime_ns > wg->get_vruntime_ns()) {
            min_wg = wg;
            min_vruntime_ns = wg->get_vruntime_ns();
        }
    }
    return min_wg;
}

int64_t DriverQueueWithWorkGroup::_ideal_runtime_ns(workgroup::WorkGroup* wg) {
    return DISPATCH_PERIOD_PER_WG_NS * _wgs.size() * wg->get_cpu_limit() / _sum_cpu_limit;
}

} // namespace starrocks::pipeline
