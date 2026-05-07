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

#include "common/bthread_timer.h"

namespace starrocks::pipeline {
using TaskId = BthreadTimerTaskId;

class PipelineTimer;

class LightTimerTask : public BthreadLightTimerTask {
public:
    virtual ~LightTimerTask() = default;
};

class PipelineTimerTask : public BthreadTimerTask {
public:
    virtual ~PipelineTimerTask() = default;

    void unschedule(PipelineTimer* timer);
};

class PipelineTimer : public BthreadTimer {
public:
    PipelineTimer() : BthreadTimer("pipeline_timer") {}
    ~PipelineTimer() noexcept = default;
};

inline void PipelineTimerTask::unschedule(PipelineTimer* timer) {
    BthreadTimerTask::unschedule(timer);
}
} // namespace starrocks::pipeline
