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

#include <limits>

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
    _mark_non_empty(level);
}

void LockFreeDriverQueue::put_back(DriverRawPtr driver) {
    int level = _compute_driver_level(driver);
    driver->set_driver_queue_level(level);
    _queue.enqueue(driver, level);
    _mark_non_empty(level);
}

bool LockFreeDriverQueue::try_take(DriverRawPtr& driver, int worker_id) {
    uint8_t bitmap = _non_empty_bitmap.load(std::memory_order_acquire);
    if (bitmap != 0) {
        int best_level = _find_best_level(bitmap);
        if (best_level >= 0 && _try_take_from_levels(bitmap, best_level, worker_id % QUEUE_SIZE, driver,
                                                     [this, worker_id](int level, DriverRawPtr& d) {
                                                         return _queue.try_dequeue(level, d, worker_id);
                                                     })) {
            return true;
        }
    }

    // Bitmap may be stale due to a concurrent clear racing with an enqueue's
    // _mark_non_empty (which saw the bit already set and skipped the fetch_or).
    // Fall back to scanning all levels directly to avoid orphaning items.
    return _fallback_try_take(
            driver, [this, worker_id](int level, DriverRawPtr& d) { return _queue.try_dequeue(level, d, worker_id); });
}

bool LockFreeDriverQueue::try_take(DriverRawPtr& driver) {
    uint8_t bitmap = _non_empty_bitmap.load(std::memory_order_acquire);
    if (bitmap != 0) {
        int best_level = _find_best_level(bitmap);
        if (best_level >= 0 && _try_take_from_levels(bitmap, best_level, 0, driver, [this](int level, DriverRawPtr& d) {
                return _queue.try_dequeue(level, d);
            })) {
            return true;
        }
    }

    return _fallback_try_take(driver, [this](int level, DriverRawPtr& d) { return _queue.try_dequeue(level, d); });
}

template <typename DequeueFunc>
bool LockFreeDriverQueue::_try_take_from_levels(uint8_t bitmap, int best_level, int start, DriverRawPtr& driver,
                                                DequeueFunc&& dequeue) {
    // Try the best level first — this succeeds most of the time.
    if (dequeue(best_level, driver)) {
        return true;
    }

    // Best level was empty (lost race). Try remaining levels with bitmap bit set.
    // Track which levels failed so we can batch-clear their bits at the end.
    //
    // NOTE: We do NOT clear bits when dequeue succeeds at a non-best level.
    // Reason: an enqueue concurrent with this loop may have observed the bitmap
    // bit as still set (because we haven't cleared it yet) and skipped fetch_or
    // in _mark_non_empty. If we then clear that bit, the enqueued item becomes
    // hidden from future try_take calls (the wrapper returns true and skips its
    // fallback). It is acceptable for stale "set" bits to remain — try_take will
    // just attempt an empty level and fall through harmlessly. Stale "clear"
    // bits, on the other hand, can hide items.
    uint8_t to_clear = static_cast<uint8_t>(1u << best_level);
    for (int j = 0; j < QUEUE_SIZE; ++j) {
        int i = (start + j) % QUEUE_SIZE;
        if (i == best_level || !(bitmap & (1u << i))) continue;
        if (dequeue(i, driver)) {
            return true;
        }
        to_clear |= static_cast<uint8_t>(1u << i);
    }

    // All bitmap-indicated levels were empty in this attempt. Clear their bits
    // so future calls can short-circuit on bitmap == 0. The wrapper's fallback
    // path will rescue any items hidden by a racing enqueue.
    _non_empty_bitmap.fetch_and(static_cast<uint8_t>(~to_clear), std::memory_order_relaxed);
    return false;
}

template <typename DequeueFunc>
bool LockFreeDriverQueue::_fallback_try_take(DriverRawPtr& driver, DequeueFunc&& dequeue) {
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        if (dequeue(i, driver)) {
            _mark_non_empty(i);
            return true;
        }
    }
    return false;
}

int LockFreeDriverQueue::_find_best_level(uint8_t bitmap) const {
    int best_level = -1;
    double min_time = std::numeric_limits<double>::max();
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        if (!(bitmap & (1u << i))) continue;
        double weighted_time = static_cast<double>(_level_stats[i].accu_time_ns.load(std::memory_order_relaxed)) /
                               _level_stats[i].factor;
        if (weighted_time < min_time) {
            min_time = weighted_time;
            best_level = i;
        }
    }
    return best_level;
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

void LockFreeDriverQueue::_mark_non_empty(int level) {
    uint8_t mask = static_cast<uint8_t>(1u << level);
    if ((_non_empty_bitmap.load(std::memory_order_relaxed) & mask) == 0) {
        _non_empty_bitmap.fetch_or(mask, std::memory_order_relaxed);
    }
}

} // namespace starrocks::pipeline
