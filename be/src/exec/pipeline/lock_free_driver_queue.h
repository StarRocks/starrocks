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

#include <algorithm>
#include <array>
#include <atomic>

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/work_stealing_queue.h"

namespace starrocks::pipeline {

// LockFreeDriverQueue is a lock-free per-workgroup MLFQ (Multi-Level Feedback Queue)
// for scheduling pipeline drivers. It replaces QuerySharedDriverQueue by using
// WorkStealingQueue internally and managing level selection via atomic accu_time
// counters and pre-computed weight factors.
//
// Drivers are assigned to one of QUEUE_SIZE priority levels based on their
// accumulated execution time. The dequeue operation selects the level with
// the minimum weighted accumulated time (accu_time_ns / factor), which ensures
// that short-running queries get priority while preventing starvation of
// long-running ones.
class LockFreeDriverQueue {
public:
    static constexpr int QUEUE_SIZE = 8;

    explicit LockFreeDriverQueue(int num_workers);

    // Non-copyable, non-movable.
    LockFreeDriverQueue(const LockFreeDriverQueue&) = delete;
    LockFreeDriverQueue& operator=(const LockFreeDriverQueue&) = delete;
    LockFreeDriverQueue(LockFreeDriverQueue&&) = delete;
    LockFreeDriverQueue& operator=(LockFreeDriverQueue&&) = delete;

    // Enqueue a driver. Computes the MLFQ level from the driver's accumulated
    // execution time and enqueues to the corresponding level.
    // Worker thread variant uses pre-allocated ProducerToken for reduced contention.
    void put_back(DriverRawPtr driver, int worker_id);
    // External thread variant uses implicit producer path.
    void put_back(DriverRawPtr driver);

    // Dequeue a driver. Selects the level with minimum accu_time_ns/factor
    // among non-empty levels. Returns true if a driver was dequeued.
    bool try_take(DriverRawPtr& driver);

    // Update per-level accumulated time after a driver completes execution.
    void update_statistics(int level, int64_t execution_time_ns);

    // Returns the approximate total number of enqueued drivers across all levels.
    size_t size() const;

private:
    // Compute the appropriate MLFQ level for a driver based on its accumulated
    // execution time. Drivers that have consumed more CPU time are placed at
    // higher (lower-priority) levels.
    int _compute_driver_level(DriverRawPtr driver) const;

    WorkStealingQueue<DriverRawPtr, QUEUE_SIZE> _queue;

    // Per-level statistics, cache-line aligned to prevent false sharing.
    struct alignas(64) LevelStats {
        std::atomic<int64_t> accu_time_ns{0};
        double factor{1.0};
    };
    std::array<LevelStats, QUEUE_SIZE> _level_stats;

    // When the driver at the i-th level costs _level_time_slices[i],
    // it will move to (i+1)-th level.
    int64_t _level_time_slices[QUEUE_SIZE];

    const int64_t LEVEL_TIME_SLICE_BASE_NS;
    const double RATIO_OF_ADJACENT_QUEUE;
};

} // namespace starrocks::pipeline
