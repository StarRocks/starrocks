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

#include "common/thread/blocking_priority_queue.hpp"
#include "compute_env/workgroup/scan_task_queue.h"

namespace starrocks::workgroup {

class PriorityScanTaskQueue final : public ScanTaskQueue {
public:
    explicit PriorityScanTaskQueue(size_t max_elements);
    ~PriorityScanTaskQueue() override = default;

    void close() override { _queue.shutdown(); }

    StatusOr<ScanTask> take() override;
    bool try_take(ScanTask* task);
    bool try_offer(ScanTask task) override;
    void force_put(ScanTask task) override;

    size_t size() const override { return _queue.get_size(); }

    void update_statistics(ScanTask& task, int64_t runtime_ns) override {}
    bool should_yield(const WorkGroupScanSchedEntity* scan_sched_entity,
                      int64_t unaccounted_runtime_ns) const override {
        return false;
    }

private:
    BlockingPriorityQueue<ScanTask> _queue;
};

} // namespace starrocks::workgroup
