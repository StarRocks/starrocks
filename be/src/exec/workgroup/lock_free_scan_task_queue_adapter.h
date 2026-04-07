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

#include "exec/workgroup/lock_free_work_group_scan_task_queue.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::workgroup {

/// Adapter that wraps LockFreeWorkGroupScanTaskQueue to satisfy the ScanTaskQueue interface.
class LockFreeScanTaskQueueAdapter : public ScanTaskQueue {
public:
    LockFreeScanTaskQueueAdapter(ScanSchedEntityType sched_entity_type, int num_workers)
            : _impl(sched_entity_type, num_workers) {}

    void close() override { _impl.close(); }

    StatusOr<ScanTask> take() override {
        ScanTask task;
        if (_impl.take(task, /*blocking=*/true)) {
            return std::move(task);
        }
        return Status::Cancelled("Shutdown");
    }

    bool try_offer(ScanTask task) override {
        _set_wg_in_queue(task);
        return _impl.try_offer(std::move(task));
    }

    void force_put(ScanTask task) override {
        _set_wg_in_queue(task);
        _impl.force_put(std::move(task));
    }

    size_t size() const override { return _impl.size(); }

    void update_statistics(ScanTask& task, int64_t runtime_ns) override { _impl.update_statistics(task, runtime_ns); }

    bool should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const override {
        return _impl.should_yield(wg, unaccounted_runtime_ns);
    }

private:
    void _set_wg_in_queue(const ScanTask& task) {
        if (task.workgroup) {
            auto* entity = const_cast<WorkGroupScanSchedEntity*>(
                    _impl.sched_entity_type() == ScanSchedEntityType::CONNECTOR
                            ? task.workgroup->connector_scan_sched_entity()
                            : task.workgroup->scan_sched_entity());
            entity->set_in_queue(this);
        }
    }

    LockFreeWorkGroupScanTaskQueue _impl;
};

} // namespace starrocks::workgroup
