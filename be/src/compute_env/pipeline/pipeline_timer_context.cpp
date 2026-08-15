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

#include "compute_env/pipeline/pipeline_timer_context.h"

#include "butil/time.h"
#include "common/status.h"
#include "gutil/casts.h"

namespace starrocks::pipeline {

void RFScanWaitTimeout::Run() {
    if (_all_rf_timeout) {
        _timeout.notify_runtime_filter_timeout();
    } else {
        _timeout.notify_source_observers();
    }
}

PipelineTimerContext::~PipelineTimerContext() {
    clear_rf_timeout_tasks();
}

Status PipelineTimerContext::schedule(PipelineTimerTask* task, timespec abstime) {
    if (_timer == nullptr) {
        return Status::InternalError("Pipeline timer is not initialized");
    }
    return _timer->schedule(task, abstime);
}

void PipelineTimerContext::unschedule_and_join(PipelineTimerTask* task) {
    if (_timer == nullptr || task == nullptr) {
        return;
    }
    task->unschedule_and_join(_timer);
}

void PipelineTimerContext::add_rf_timeout_observer(RuntimeState* state, PipelineObserver* observer,
                                                   uint64_t timeout_ns) {
    RFScanWaitTimeout* task;
    if (auto iter = _rf_timeout_tasks.find(timeout_ns); iter != _rf_timeout_tasks.end()) {
        task = down_cast<RFScanWaitTimeout*>(iter->second.get());
    } else {
        auto timeout_task = std::make_shared<RFScanWaitTimeout>();
        task = timeout_task.get();
        _rf_timeout_tasks.emplace(timeout_ns, std::move(timeout_task));
    }
    task->add_observer(state, observer);
}

Status PipelineTimerContext::submit_rf_timeout_tasks() {
    timespec tm = butil::microseconds_to_timespec(butil::gettimeofday_us());
    for (const auto& [delta_ns, task] : _rf_timeout_tasks) {
        timespec abstime = tm;
        abstime.tv_nsec += delta_ns;
        butil::timespec_normalize(&abstime);
        RETURN_IF_ERROR(schedule(task.get(), abstime));
    }
    return Status::OK();
}

void PipelineTimerContext::clear_rf_timeout_tasks() {
    for (auto& [_, task] : _rf_timeout_tasks) {
        unschedule_and_join(task.get());
        task.reset();
    }
    _rf_timeout_tasks.clear();
}

} // namespace starrocks::pipeline
