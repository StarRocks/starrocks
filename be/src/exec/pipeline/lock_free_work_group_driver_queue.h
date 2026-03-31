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

#include <atomic>
#include <mutex>
#include <unordered_map>

#include "base/concurrency/moodycamel/lightweightsemaphore.h"
#include "base/phmap/phmap.h"
#include "exec/pipeline/lock_free_driver_queue.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::pipeline {

// LockFreeWorkGroupDriverQueue is the cross-workgroup composition layer that
// manages multiple LockFreeDriverQueue instances (one per workgroup). It replaces
// WorkGroupDriverQueue with a lock-free design using:
//   - Atomic vruntime scanning for workgroup selection
//   - moodycamel::LightweightSemaphore for blocking
//   - phmap::parallel_flat_hash_set for concurrent cancel tracking
//
// Each workgroup gets its own LockFreeDriverQueue that is lazily created and
// stored in an internal map. Workgroup selection picks the workgroup with the
// minimum vruntime among those with queued drivers.
class LockFreeWorkGroupDriverQueue {
public:
    explicit LockFreeWorkGroupDriverQueue(int num_workers);
    ~LockFreeWorkGroupDriverQueue() = default;

    // Non-copyable, non-movable.
    LockFreeWorkGroupDriverQueue(const LockFreeWorkGroupDriverQueue&) = delete;
    LockFreeWorkGroupDriverQueue& operator=(const LockFreeWorkGroupDriverQueue&) = delete;
    LockFreeWorkGroupDriverQueue(LockFreeWorkGroupDriverQueue&&) = delete;
    LockFreeWorkGroupDriverQueue& operator=(LockFreeWorkGroupDriverQueue&&) = delete;

    // Enqueue a driver. Worker thread variant uses worker_id for reduced contention.
    void put_back(DriverRawPtr driver, int worker_id);
    // External thread variant (e.g., from poller or new driver submission).
    void put_back(DriverRawPtr driver);

    // Dequeue a driver. Selects the workgroup with minimum vruntime among those
    // with queued drivers, then dequeues from that workgroup's LockFreeDriverQueue.
    // If blocking=true, waits on the semaphore until a driver is available or
    // the queue is closed.
    // Returns true if a driver was obtained; false if non-blocking and empty, or closed.
    bool take(DriverRawPtr& driver, bool blocking);

    // Mark a driver for cancellation. The driver will be returned from take()
    // with DriverState::CANCELED set.
    void cancel(DriverRawPtr driver);

    // Update per-level MLFQ statistics and workgroup vruntime after a driver
    // completes an execution quantum.
    void update_statistics(const DriverRawPtr driver);

    // Close the queue and wake all blocked consumers.
    void close();

    // Returns the approximate total number of enqueued drivers across all workgroups.
    size_t size() const;

private:
    // Get or create the LockFreeDriverQueue for a workgroup.
    LockFreeDriverQueue* _get_or_create_wg_queue(workgroup::WorkGroup* wg);

    // Scan active workgroups for the one with minimum vruntime that has queued drivers.
    // Returns nullptr if no workgroup has queued drivers.
    workgroup::WorkGroupDriverSchedEntity* _pick_next_wg();

    int _num_workers;
    std::atomic<size_t> _num_drivers{0};
    std::atomic<bool> _closed{false};

    moodycamel::LightweightSemaphore _sema;

    // Cancel set using phmap for concurrent access. Drivers in this set will have
    // their state set to CANCELED when dequeued.
    phmap::parallel_flat_hash_set<DriverRawPtr, phmap::priv::hash_default_hash<DriverRawPtr>,
                                  phmap::priv::hash_default_eq<DriverRawPtr>, phmap::priv::Allocator<DriverRawPtr>, 4,
                                  std::mutex>
            _cancel_set;

    // Per-workgroup LockFreeDriverQueue instances. Protected by _wg_queues_mutex.
    // We maintain our own mapping because the WorkGroupDriverSchedEntity's queue()
    // returns DriverQueue* and LockFreeDriverQueue is not a subclass of DriverQueue.
    // This will be reconciled during integration (Task 8).
    mutable std::mutex _wg_queues_mutex;
    std::unordered_map<workgroup::WorkGroup*, std::unique_ptr<LockFreeDriverQueue>> _wg_queues;
};

} // namespace starrocks::pipeline
