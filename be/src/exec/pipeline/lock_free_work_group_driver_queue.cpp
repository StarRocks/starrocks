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

#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

LockFreeWorkGroupDriverQueue::LockFreeWorkGroupDriverQueue(int num_workers) : _num_workers(num_workers) {}

void LockFreeWorkGroupDriverQueue::put_back(DriverRawPtr driver, int worker_id) {
    auto* wg_queue = _get_or_create_wg_queue(driver->workgroup());
    wg_queue->put_back(driver, worker_id);
    _num_drivers.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

void LockFreeWorkGroupDriverQueue::put_back(DriverRawPtr driver) {
    auto* wg_queue = _get_or_create_wg_queue(driver->workgroup());
    wg_queue->put_back(driver);
    _num_drivers.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

bool LockFreeWorkGroupDriverQueue::take(DriverRawPtr& driver, bool blocking) {
    while (true) {
        if (_closed.load(std::memory_order_acquire)) {
            return false;
        }

        auto* wg_entity = _pick_next_wg();
        if (wg_entity != nullptr) {
            // Find the corresponding LockFreeDriverQueue for this workgroup.
            LockFreeDriverQueue* wg_queue = nullptr;
            {
                std::lock_guard lock(_wg_queues_mutex);
                auto it = _wg_queues.find(wg_entity->workgroup());
                if (it != _wg_queues.end()) {
                    wg_queue = it->second.get();
                }
            }

            if (wg_queue != nullptr && wg_queue->try_take(driver)) {
                _num_drivers.fetch_sub(1, std::memory_order_relaxed);
                // Check if this driver was cancelled.
                if (_cancel_set.erase(driver)) {
                    driver->set_driver_state(DriverState::CANCELED);
                }
                return true;
            }
        }

        if (!blocking) {
            return false;
        }
        _sema.wait();
    }
}

void LockFreeWorkGroupDriverQueue::cancel(DriverRawPtr driver) {
    _cancel_set.insert(driver);
    // Signal to wake a consumer that might be blocking, so it can process
    // the cancelled driver when it eventually dequeues it.
    _sema.signal();
}

void LockFreeWorkGroupDriverQueue::update_statistics(const DriverRawPtr driver) {
    int level = driver->get_driver_queue_level();
    int64_t time_spent = driver->driver_acct().get_last_time_spent();

    // Update per-level MLFQ stats in the workgroup's queue.
    auto* wg_queue = _get_or_create_wg_queue(driver->workgroup());
    wg_queue->update_statistics(level, time_spent);

    // Update workgroup vruntime.
    auto* entity = driver->workgroup()->driver_sched_entity();
    entity->incr_runtime_ns(time_spent);
}

void LockFreeWorkGroupDriverQueue::close() {
    _closed.store(true, std::memory_order_release);
    // Wake all potential waiters. Use a large count to ensure all blocked
    // consumer threads are woken.
    _sema.signal(1024);
}

size_t LockFreeWorkGroupDriverQueue::size() const {
    return _num_drivers.load(std::memory_order_relaxed);
}

LockFreeDriverQueue* LockFreeWorkGroupDriverQueue::_get_or_create_wg_queue(workgroup::WorkGroup* wg) {
    std::lock_guard lock(_wg_queues_mutex);
    auto it = _wg_queues.find(wg);
    if (it != _wg_queues.end()) {
        return it->second.get();
    }
    auto [inserted_it, ok] = _wg_queues.emplace(wg, std::make_unique<LockFreeDriverQueue>(_num_workers));
    return inserted_it->second.get();
}

workgroup::WorkGroupDriverSchedEntity* LockFreeWorkGroupDriverQueue::_pick_next_wg() {
    auto* wg_manager = ExecEnv::GetInstance()->workgroup_manager();
    if (wg_manager == nullptr) {
        return nullptr;
    }

    workgroup::WorkGroupDriverSchedEntity* best = nullptr;
    int64_t best_vruntime = INT64_MAX;

    // Iterate all registered workgroups and find the one with minimum vruntime
    // that has drivers queued in our internal map.
    wg_manager->for_each_workgroup([&](const workgroup::WorkGroup& wg) {
        const auto* entity = wg.driver_sched_entity();

        // Check if this workgroup has a queue in our map with queued drivers.
        LockFreeDriverQueue* queue = nullptr;
        {
            std::lock_guard lock(_wg_queues_mutex);
            auto it = _wg_queues.find(const_cast<workgroup::WorkGroup*>(&wg));
            if (it != _wg_queues.end()) {
                queue = it->second.get();
            }
        }

        if (queue != nullptr && queue->size() > 0) {
            int64_t vrt = entity->vruntime_ns();
            if (vrt < best_vruntime) {
                best_vruntime = vrt;
                // const_cast is safe here: the entity is owned by a non-const WorkGroup;
                // we receive const ref from for_each_workgroup's consumer signature.
                best = const_cast<workgroup::WorkGroup&>(wg).driver_sched_entity();
            }
        }
    });

    return best;
}

} // namespace starrocks::pipeline
