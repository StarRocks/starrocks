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

#include "exec/pipeline/multi_level_concurrent_queue.h"
#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline {

// LockFreeDriverQueue is a lock-free per-workgroup MLFQ (Multi-Level Feedback Queue)
// for scheduling pipeline drivers. It replaces QuerySharedDriverQueue by using
// MultiLevelConcurrentQueue internally and managing level selection via atomic accu_time
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
    // Worker thread variant uses ConsumerToken for even load distribution.
    bool try_take(DriverRawPtr& driver, int worker_id);
    // Without worker_id (external or fallback).
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

    void _mark_non_empty(int level);

    // Find the non-empty level with minimum weighted accu_time among bits set in bitmap.
    // Returns -1 if no candidate found.
    int _find_best_level(uint8_t bitmap) const;

    // Try best_level first, then fallback to remaining levels in bitmap.
    // DequeueFunc: (int level, DriverRawPtr& driver) -> bool
    template <typename DequeueFunc>
    bool _try_take_from_levels(uint8_t bitmap, int best_level, int start, DriverRawPtr& driver, DequeueFunc&& dequeue);

    // Fallback: scan all levels directly, ignoring bitmap.
    // Used when bitmap-guided dequeue fails to guard against stale bitmap bits.
    template <typename DequeueFunc>
    bool _fallback_try_take(DriverRawPtr& driver, DequeueFunc&& dequeue);

    MultiLevelConcurrentQueue<DriverRawPtr, QUEUE_SIZE> _queue;

    // Bitmap of levels that are known to be non-empty.
    // bit i = 1 means level i MAY have items (no false negatives).
    // bit i = 0 means level i is DEFINITELY empty.
    // Replaces 8 × O(P) size_approx() calls with a single atomic read on the hot path.
    std::atomic<uint8_t> _non_empty_bitmap{0};

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
