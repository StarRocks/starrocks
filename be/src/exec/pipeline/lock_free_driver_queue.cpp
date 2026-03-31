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

#include "exec/pipeline/lock_free_driver_queue.h"

#include "common/config_exec_flow_fwd.h"

namespace starrocks::pipeline {

LockFreeDriverQueue::LockFreeDriverQueue(int num_workers)
        : _queue(num_workers),
          LEVEL_TIME_SLICE_BASE_NS(config::pipeline_driver_queue_level_time_slice_base_ns),
          RATIO_OF_ADJACENT_QUEUE(config::pipeline_driver_queue_ratio_of_adjacent_queue) {
    // Initialize factors for each level. Higher priority queues (lower index)
    // get larger factors so their accumulated time is divided by more,
    // giving them proportionally more execution time before being deprioritized.
    double factor = 1.0;
    for (int i = QUEUE_SIZE - 1; i >= 0; --i) {
        _level_stats[i].factor = factor;
        factor *= RATIO_OF_ADJACENT_QUEUE;
    }

    // Initialize time slice thresholds. A driver moves from level i to level i+1
    // when its accumulated execution time exceeds _level_time_slices[i].
    // With the default base of 200ms, the thresholds are:
    // 0.2s, 0.6s, 1.2s, 2.0s, 3.0s, 4.2s, 5.6s, 7.4s
    int64_t time_slice = 0;
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        time_slice += LEVEL_TIME_SLICE_BASE_NS * (i + 1);
        _level_time_slices[i] = time_slice;
    }
}

void LockFreeDriverQueue::put_back(DriverRawPtr driver, int worker_id) {
    int level = _compute_driver_level(driver);
    driver->set_driver_queue_level(level);
    _queue.enqueue(driver, level, worker_id);
}

void LockFreeDriverQueue::put_back(DriverRawPtr driver) {
    int level = _compute_driver_level(driver);
    driver->set_driver_queue_level(level);
    _queue.enqueue(driver, level);
}

bool LockFreeDriverQueue::try_take(DriverRawPtr& driver) {
    // Build a sorted order of levels by weighted accu_time.
    // With only 8 elements, this is cheaper than the CAS operations in try_dequeue.
    std::array<std::pair<double, int>, QUEUE_SIZE> scored;
    int non_empty_count = 0;
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        if (_queue.empty(i)) continue;
        double weighted_time =
                static_cast<double>(_level_stats[i].accu_time_ns.load(std::memory_order_relaxed)) /
                _level_stats[i].factor;
        scored[non_empty_count++] = {weighted_time, i};
    }

    if (non_empty_count == 0) return false;

    // Sort only the non-empty entries (at most 8 — negligible overhead).
    std::sort(scored.begin(), scored.begin() + non_empty_count);

    // Try levels in ascending weighted-time order.
    for (int j = 0; j < non_empty_count; ++j) {
        if (_queue.try_dequeue(scored[j].second, driver)) {
            return true;
        }
    }
    return false;
}

void LockFreeDriverQueue::update_statistics(int level, int64_t execution_time_ns) {
    DCHECK(level >= 0 && level < QUEUE_SIZE) << "level out of range: " << level;
    _level_stats[level].accu_time_ns.fetch_add(execution_time_ns, std::memory_order_relaxed);
}

size_t LockFreeDriverQueue::size() const {
    return _queue.size_approx();
}

int LockFreeDriverQueue::_compute_driver_level(DriverRawPtr driver) const {
    int64_t time_spent = driver->driver_acct().get_accumulated_time_spent();
    for (int i = driver->get_driver_queue_level(); i < QUEUE_SIZE; ++i) {
        if (time_spent < _level_time_slices[i]) {
            return i;
        }
    }
    return QUEUE_SIZE - 1;
}


} // namespace starrocks::pipeline
