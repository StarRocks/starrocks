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

#include "exec/pipeline/lock_free_work_group_driver_queue.h"

#include <limits>

#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

LockFreeWorkGroupDriverQueue::LockFreeWorkGroupDriverQueue(DriverQueueMetrics* metrics, int num_workers)
        : DriverQueue(metrics), _num_workers(num_workers) {}

void LockFreeWorkGroupDriverQueue::put_back(const DriverRawPtr driver) {
    _enqueue_driver(driver);
    auto* wg_queue = _get_or_create_wg_queue(driver->workgroup());
    wg_queue->put_back(driver);
    _num_drivers.fetch_add(1, std::memory_order_relaxed);
    _metrics->driver_queue_len.increment(1);
    _sema.signal();
}

void LockFreeWorkGroupDriverQueue::put_back(const std::vector<DriverRawPtr>& drivers) {
    for (auto* d : drivers) {
        put_back(d);
    }
}

void LockFreeWorkGroupDriverQueue::put_back_from_executor(const DriverRawPtr driver) {
    put_back(driver);
}

StatusOr<DriverRawPtr> LockFreeWorkGroupDriverQueue::take(const bool block) {
    while (true) {
        if (_closed.load(std::memory_order_acquire)) {
            return Status::Cancelled("Shutdown");
        }

        // Collect all workgroup candidates sorted by vruntime, then try each.
        // This ensures we try ALL workgroups before blocking.
        auto candidates = _pick_sorted_wgs();
        for (auto& [vruntime, wg_queue] : candidates) {
            DriverRawPtr driver = nullptr;
            if (wg_queue->try_take(driver)) {
                _num_drivers.fetch_sub(1, std::memory_order_relaxed);
                if (_cancel_set.erase(driver)) {
                    driver->set_driver_state(DriverState::CANCELED);
                }
                driver->set_in_ready(false);
                _metrics->driver_queue_len.increment(-1);
                return driver;
            }
        }

        if (!block) {
            return nullptr;
        }
        _sema.wait();
    }
}

void LockFreeWorkGroupDriverQueue::cancel(DriverRawPtr driver) {
    _cancel_set.insert(driver);
    _sema.signal();
}

void LockFreeWorkGroupDriverQueue::update_statistics(const DriverRawPtr driver) {
    int level = driver->get_driver_queue_level();
    int64_t time_spent = driver->driver_acct().get_last_time_spent();

    auto* wg_queue = _get_or_create_wg_queue(driver->workgroup());
    wg_queue->update_statistics(level, time_spent);

    auto* entity = driver->workgroup()->driver_sched_entity();
    entity->incr_runtime_ns(time_spent);
}

void LockFreeWorkGroupDriverQueue::close() {
    _closed.store(true, std::memory_order_release);
    _sema.signal(_num_workers);
}

size_t LockFreeWorkGroupDriverQueue::size() const {
    return _num_drivers.load(std::memory_order_relaxed);
}

bool LockFreeWorkGroupDriverQueue::should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const {
    if (ExecEnv::GetInstance()->workgroup_manager()->should_yield(driver->workgroup())) {
        return true;
    }
    auto* wg_entity = driver->workgroup()->driver_sched_entity();
    auto* min_entity = _min_wg_entity.load();
    return min_entity != wg_entity && min_entity &&
           min_entity->vruntime_ns() < wg_entity->vruntime_ns() + unaccounted_runtime_ns / wg_entity->cpu_weight();
}

void LockFreeWorkGroupDriverQueue::_enqueue_driver(const DriverRawPtr driver) {
    DCHECK(!driver->is_in_ready());
    driver->set_in_ready(true);
    driver->set_in_queue(this);
    driver->update_peak_driver_queue_size_counter(size());
    if (driver->workgroup()) {
        driver->workgroup()->driver_sched_entity()->set_in_queue(this);
    }
}

LockFreeDriverQueue* LockFreeWorkGroupDriverQueue::_get_or_create_wg_queue(workgroup::WorkGroup* wg) {
    // Fast path: read lock to check existing entry.
    {
        std::shared_lock read_lock(_wg_queues_mutex);
        auto it = _wg_queues.find(wg);
        if (it != _wg_queues.end()) {
            return it->second.get();
        }
    }
    // Slow path: exclusive lock to create.
    std::unique_lock write_lock(_wg_queues_mutex);
    auto it = _wg_queues.find(wg);
    if (it != _wg_queues.end()) {
        return it->second.get();
    }
    auto [inserted_it, ok] = _wg_queues.emplace(wg, std::make_unique<LockFreeDriverQueue>(_num_workers));
    return inserted_it->second.get();
}

LockFreeWorkGroupDriverQueue::CandidateList LockFreeWorkGroupDriverQueue::_pick_sorted_wgs() {
    CandidateList candidates;

    // Take a snapshot of all workgroup queues under the read lock (one acquisition).
    std::vector<std::pair<workgroup::WorkGroup*, LockFreeDriverQueue*>> snapshot;
    {
        std::shared_lock read_lock(_wg_queues_mutex);
        snapshot.reserve(_wg_queues.size());
        for (auto& [wg, queue] : _wg_queues) {
            if (queue->size() > 0) {
                snapshot.emplace_back(wg, queue.get());
            }
        }
    }

    // Now score by vruntime without holding any lock.
    candidates.reserve(snapshot.size());
    for (auto& [wg, queue] : snapshot) {
        int64_t vrt = wg->driver_sched_entity()->vruntime_ns();
        candidates.push_back({vrt, queue});
    }

    // Sort by vruntime ascending (min first).
    std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

    // Update min entity cache for should_yield().
    if (!snapshot.empty()) {
        workgroup::WorkGroupDriverSchedEntity* min_entity = nullptr;
        int64_t min_vrt = std::numeric_limits<int64_t>::max();
        for (auto& [wg, queue] : snapshot) {
            auto* entity = wg->driver_sched_entity();
            if (entity->vruntime_ns() < min_vrt) {
                min_vrt = entity->vruntime_ns();
                min_entity = entity;
            }
        }
        _min_wg_entity.store(min_entity, std::memory_order_relaxed);
    } else {
        _min_wg_entity.store(nullptr, std::memory_order_relaxed);
    }

    return candidates;
}

} // namespace starrocks::pipeline
