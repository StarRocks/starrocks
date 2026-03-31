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
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "base/concurrency/moodycamel/lightweightsemaphore.h"
#include "base/phmap/phmap.h"
#include "exec/pipeline/lock_free_driver_queue.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::pipeline {

/// LockFreeWorkGroupDriverQueue is the cross-workgroup composition layer.
/// It manages per-workgroup LockFreeDriverQueue instances and provides:
///   - Workgroup selection by scanning vruntimes (sorted candidate list)
///   - Blocking via LightweightSemaphore
///   - Cancel via phmap::parallel_flat_hash_set
///
/// The wg_queues map uses a shared_mutex for read-heavy access: lookups
/// are concurrent (shared lock), creation is exclusive. The hot path (take)
/// takes a single shared lock snapshot, then operates lock-free.
class LockFreeWorkGroupDriverQueue {
public:
    explicit LockFreeWorkGroupDriverQueue(int num_workers);
    ~LockFreeWorkGroupDriverQueue() = default;

    LockFreeWorkGroupDriverQueue(const LockFreeWorkGroupDriverQueue&) = delete;
    LockFreeWorkGroupDriverQueue& operator=(const LockFreeWorkGroupDriverQueue&) = delete;

    void put_back(DriverRawPtr driver, int worker_id);
    void put_back(DriverRawPtr driver);

    /// Dequeue. Tries ALL workgroups in ascending vruntime order before blocking.
    bool take(DriverRawPtr& driver, bool blocking);

    void cancel(DriverRawPtr driver);

    void update_statistics(const DriverRawPtr driver);

    void close();

    size_t size() const;

private:
    using CandidateList = std::vector<std::pair<int64_t, LockFreeDriverQueue*>>;

    LockFreeDriverQueue* _get_or_create_wg_queue(workgroup::WorkGroup* wg);

    /// Build a sorted list of (vruntime, queue) for all workgroups with queued drivers.
    /// Takes one shared_lock snapshot, then releases it before returning.
    CandidateList _pick_sorted_wgs();

    int _num_workers;
    std::atomic<size_t> _num_drivers{0};
    std::atomic<bool> _closed{false};

    moodycamel::LightweightSemaphore _sema;

    phmap::parallel_flat_hash_set<DriverRawPtr, phmap::priv::hash_default_hash<DriverRawPtr>,
                                  phmap::priv::hash_default_eq<DriverRawPtr>, phmap::priv::Allocator<DriverRawPtr>, 4,
                                  std::mutex>
            _cancel_set;

    // Per-workgroup queues. Shared_mutex: reads are concurrent, writes are exclusive.
    mutable std::shared_mutex _wg_queues_mutex;
    std::unordered_map<workgroup::WorkGroup*, std::unique_ptr<LockFreeDriverQueue>> _wg_queues;
};

} // namespace starrocks::pipeline
