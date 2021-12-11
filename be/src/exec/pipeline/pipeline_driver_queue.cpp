// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_driver_queue.h"

#include "gutil/strings/substitute.h"
namespace starrocks::pipeline {

void QuerySharedDriverQueue::put_back(const DriverRawPtr driver) {
    int level = driver->driver_acct().get_level();
    {
        std::lock_guard<std::mutex> lock(_global_mutex);
        _queues[level % QUEUE_SIZE].queue.emplace_back(driver);
        ++_size;
    }
}

void QuerySharedDriverQueue::put_back(const std::vector<DriverRawPtr>& drivers) {
    std::vector<int> levels(drivers.size());
    for (int i = 0; i < drivers.size(); i++) {
        levels[i] = drivers[i]->driver_acct().get_level();
    }

    std::lock_guard<std::mutex> lock(_global_mutex);
    _size += drivers.size();
    for (int i = 0; i < drivers.size(); i++) {
        _queues[levels[i] % QUEUE_SIZE].queue.emplace_back(drivers[i]);
    }
}

DriverRawPtr QuerySharedDriverQueue::take_own(size_t* queue_index) {
    if (_size == 0) {
        return nullptr;
    }

    return take(queue_index);
}

DriverRawPtr QuerySharedDriverQueue::take(size_t* queue_index) {
    // -1 means no candidates; else has candidate.
    int queue_idx = -1;
    double target_accu_time = 0;
    DriverRawPtr driver_ptr;

    {
        std::unique_lock<std::mutex> lock(_global_mutex);

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

        if (queue_idx < 0) {
            return nullptr;
        }

        // record queue's index to accumulate time for it.
        *queue_index = queue_idx;
        driver_ptr = _queues[queue_idx].queue.front();
        _queues[queue_idx].queue.pop_front();

        --_size;

        // next pipeline driver to execute.
        return driver_ptr;
    }
}

SubQuerySharedDriverQueue* QuerySharedDriverQueue::get_sub_queue(size_t index) {
    return _queues + index;
}

} // namespace starrocks::pipeline
