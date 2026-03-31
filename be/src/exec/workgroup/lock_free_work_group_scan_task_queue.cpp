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

#include "exec/workgroup/lock_free_work_group_scan_task_queue.h"

#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::workgroup {

LockFreeWorkGroupScanTaskQueue::LockFreeWorkGroupScanTaskQueue(int num_workers) : _num_workers(num_workers) {}

bool LockFreeWorkGroupScanTaskQueue::try_offer(ScanTask task, int worker_id) {
    auto* wg_queue = _get_or_create_wg_queue(task.workgroup.get());
    wg_queue->try_offer(std::move(task), worker_id);
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
    return true;
}

bool LockFreeWorkGroupScanTaskQueue::try_offer(ScanTask task) {
    auto* wg_queue = _get_or_create_wg_queue(task.workgroup.get());
    wg_queue->try_offer(std::move(task));
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
    return true;
}

void LockFreeWorkGroupScanTaskQueue::force_put(ScanTask task, int worker_id) {
    auto* wg_queue = _get_or_create_wg_queue(task.workgroup.get());
    wg_queue->force_put(std::move(task), worker_id);
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

void LockFreeWorkGroupScanTaskQueue::force_put(ScanTask task) {
    auto* wg_queue = _get_or_create_wg_queue(task.workgroup.get());
    wg_queue->force_put(std::move(task));
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

bool LockFreeWorkGroupScanTaskQueue::take(ScanTask& task, bool blocking) {
    while (true) {
        if (_closed.load(std::memory_order_acquire)) {
            return false;
        }

        auto* wg_entity = _pick_next_wg();
        if (wg_entity != nullptr) {
            // Find the corresponding LockFreeScanTaskQueue for this workgroup.
            LockFreeScanTaskQueue* wg_queue = nullptr;
            {
                std::lock_guard lock(_wg_queues_mutex);
                auto it = _wg_queues.find(wg_entity->workgroup());
                if (it != _wg_queues.end()) {
                    wg_queue = it->second.get();
                }
            }

            if (wg_queue != nullptr && wg_queue->try_take(task)) {
                _num_tasks.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
        }

        if (!blocking) {
            return false;
        }
        _sema.wait();
    }
}

void LockFreeWorkGroupScanTaskQueue::update_statistics(ScanTask& task, int64_t runtime_ns) {
    // Update workgroup scan vruntime.
    auto* entity = task.workgroup->scan_sched_entity();
    entity->incr_runtime_ns(runtime_ns);
}

void LockFreeWorkGroupScanTaskQueue::close() {
    _closed.store(true, std::memory_order_release);
    // Wake all potential waiters. Use a large count to ensure all blocked
    // consumer threads are woken.
    _sema.signal(1024);
}

size_t LockFreeWorkGroupScanTaskQueue::size() const {
    return _num_tasks.load(std::memory_order_relaxed);
}

LockFreeScanTaskQueue* LockFreeWorkGroupScanTaskQueue::_get_or_create_wg_queue(WorkGroup* wg) {
    std::lock_guard lock(_wg_queues_mutex);
    auto it = _wg_queues.find(wg);
    if (it != _wg_queues.end()) {
        return it->second.get();
    }
    auto [inserted_it, ok] = _wg_queues.emplace(wg, std::make_unique<LockFreeScanTaskQueue>(_num_workers));
    return inserted_it->second.get();
}

WorkGroupScanSchedEntity* LockFreeWorkGroupScanTaskQueue::_pick_next_wg() {
    auto* wg_manager = ExecEnv::GetInstance()->workgroup_manager();
    if (wg_manager == nullptr) {
        return nullptr;
    }

    WorkGroupScanSchedEntity* best = nullptr;
    int64_t best_vruntime = INT64_MAX;

    // Iterate all registered workgroups and find the one with minimum vruntime
    // that has tasks queued in our internal map.
    wg_manager->for_each_workgroup([&](const WorkGroup& wg) {
        const auto* entity = wg.scan_sched_entity();

        // Check if this workgroup has a queue in our map with queued tasks.
        LockFreeScanTaskQueue* queue = nullptr;
        {
            std::lock_guard lock(_wg_queues_mutex);
            auto it = _wg_queues.find(const_cast<WorkGroup*>(&wg));
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
                best = const_cast<WorkGroup&>(wg).scan_sched_entity();
            }
        }
    });

    return best;
}

} // namespace starrocks::workgroup
