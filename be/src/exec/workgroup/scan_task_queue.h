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

#include <cstddef>
#include <cstdint>

#include "common/statusor.h"
#include "exec/workgroup/scan_task.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

/// There are two types of ScanTaskQueue:
/// - WorkGroupScanTaskQueue, which is a two-level queue.
///   - The first level selects the workgroup with the shortest execution time.
///   - The second level selects an appropriate task using either PriorityScanTaskQueue.
/// - PriorityScanTaskQueue, which prioritizes scan tasks with lower committed times.
class ScanTaskQueue {
public:
    ScanTaskQueue() = default;
    virtual ~ScanTaskQueue() = default;

    virtual void close() = 0;

    virtual StatusOr<ScanTask> take() = 0;
    virtual StatusOr<ScanTask> take(int worker_id) { return take(); }
    virtual bool try_offer(ScanTask task) = 0;
    virtual void force_put(ScanTask task) = 0;

    virtual size_t size() const = 0;
    bool empty() const { return size() == 0; }

    virtual void update_statistics(ScanTask& task, int64_t runtime_ns) = 0;
    virtual bool should_yield(const WorkGroupScanSchedEntity* scan_sched_entity,
                              int64_t unaccounted_runtime_ns) const = 0;
};

} // namespace starrocks::workgroup
