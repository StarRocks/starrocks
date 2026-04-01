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

#include "exec/pipeline/lock_free_work_group_driver_queue.h"
#include "exec/pipeline/pipeline_driver_queue.h"

namespace starrocks::pipeline {

/// Adapter that wraps LockFreeWorkGroupDriverQueue to satisfy the DriverQueue interface.
/// This allows it to be used as a drop-in replacement in GlobalDriverExecutor.
class LockFreeDriverQueueAdapter : public DriverQueue {
public:
    LockFreeDriverQueueAdapter(DriverQueueMetrics* metrics, int num_workers)
            : DriverQueue(metrics), _impl(num_workers) {}

    void close() override { _impl.close(); }

    void put_back(const DriverRawPtr driver) override { _impl.put_back(driver); }

    void put_back(const std::vector<DriverRawPtr>& drivers) override {
        for (auto* d : drivers) {
            _impl.put_back(d);
        }
    }

    void put_back_from_executor(const DriverRawPtr driver) override {
        // In the lock-free path, worker_id is not available through this interface.
        // Use the no-token path. For full performance, callers should be updated
        // to pass worker_id directly (done in the executor thread via the new overload).
        _impl.put_back(driver);
    }

    // Extended version with worker_id for executor threads.
    void put_back_from_executor(const DriverRawPtr driver, int worker_id) {
        _impl.put_back(driver, worker_id);
    }

    StatusOr<DriverRawPtr> take(const bool block) override {
        DriverRawPtr driver = nullptr;
        if (_impl.take(driver, block)) {
            return driver;
        }
        if (block) {
            return Status::Cancelled("Shutdown");
        }
        return nullptr;
    }

    void cancel(DriverRawPtr driver) override { _impl.cancel(driver); }

    void update_statistics(const DriverRawPtr driver) override { _impl.update_statistics(driver); }

    size_t size() const override { return _impl.size(); }

    bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override {
        // TODO: Implement vruntime-based yield check for lock-free path.
        return false;
    }

private:
    LockFreeWorkGroupDriverQueue _impl;
};

} // namespace starrocks::pipeline
