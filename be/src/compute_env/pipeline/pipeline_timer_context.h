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

#include <memory>
#include <unordered_map>

#include "common/status.h"
#include "compute_env/pipeline/pipeline_timer.h"
#include "exec/pipeline/primitives/pipeline_observer.h"

namespace starrocks {

class RuntimeState;

namespace pipeline {

// If the timeout is reached, a cancel_update event is sent to all objects observing _timeout.
class RFScanWaitTimeout final : public PipelineTimerTask {
public:
    RFScanWaitTimeout(bool all_rf_timeout = false) : _all_rf_timeout(all_rf_timeout) {}

    void add_observer(RuntimeState* state, PipelineObserver* observer) { _timeout.add_observer(state, observer); }
    void Run() override;

private:
    bool _all_rf_timeout = false;
    Observable _timeout;
};

class PipelineTimerContext {
public:
    explicit PipelineTimerContext(PipelineTimer* timer) : _timer(timer) {}
    ~PipelineTimerContext();

    PipelineTimerContext(const PipelineTimerContext&) = delete;
    PipelineTimerContext& operator=(const PipelineTimerContext&) = delete;

    Status schedule(PipelineTimerTask* task, timespec abstime);
    void unschedule_and_join(PipelineTimerTask* task);

    void add_rf_timeout_observer(RuntimeState* state, PipelineObserver* observer, uint64_t timeout_ns);
    Status submit_rf_timeout_tasks();
    void clear_rf_timeout_tasks();

private:
    PipelineTimer* _timer = nullptr;
    std::unordered_map<uint64_t, std::shared_ptr<PipelineTimerTask>> _rf_timeout_tasks;
};

using PipelineTimerContextPtr = std::shared_ptr<PipelineTimerContext>;

} // namespace pipeline
} // namespace starrocks
