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
#include <memory>
#include <vector>

#include "common/statusor.h"
#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {

class DriverQueue;
using DriverQueuePtr = std::unique_ptr<DriverQueue>;
class DriverQueueMetrics;

class DriverQueue {
public:
    DriverQueue(DriverQueueMetrics* metrics) : _metrics(metrics) {}
    virtual ~DriverQueue() = default;
    virtual void close() = 0;

    virtual void put_back(const DriverRawPtr driver) = 0;
    virtual void put_back(const std::vector<DriverRawPtr>& drivers) = 0;
    // *from_executor* means that the executor thread puts the driver back to the queue.
    virtual void put_back_from_executor(const DriverRawPtr driver) = 0;

    virtual StatusOr<DriverRawPtr> take(const bool block) = 0;
    virtual void cancel(DriverRawPtr driver) = 0;

    // Update statistics of the driver's workgroup,
    // when yielding the driver in the executor thread.
    virtual void update_statistics(const DriverRawPtr driver) = 0;

    virtual size_t size() const = 0;
    bool empty() const { return size() == 0; }

    virtual bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const = 0;

    DriverQueueMetrics* _metrics;
};

} // namespace starrocks::pipeline
