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

#include <cstdint>
#include <latch>
#include <memory>

#include "common/status.h"

namespace bthread {
class TimerThread;
}

namespace starrocks::pipeline {
using TaskId = int64_t;
class PipelineTimer;

class LightTimerTask {
public:
    virtual ~LightTimerTask() = default;

    virtual void Run() = 0;

    void set_tid(TaskId tid) { _tid = tid; }
    TaskId tid() const { return _tid; }

private:
    TaskId _tid{};
};

class PipelineTimerTask : public std::enable_shared_from_this<PipelineTimerTask> {
public:
    virtual ~PipelineTimerTask() = default;

    void doRun() {
        auto self = shared_from_this();
        Run();
        _latch.count_down();
    }

    void unschedule(PipelineTimer* timer);

    void set_tid(TaskId tid) { _tid = tid; }
    TaskId tid() const { return _tid; }

private:
    // only call when unschedule == 1
    void waitUtilFinished();

protected:
    // implement interface
    virtual void Run() = 0;

protected:
    std::latch _latch{1};
    TaskId _tid{};
};

class PipelineTimer {
public:
    PipelineTimer() = default;
    ~PipelineTimer() noexcept = default;

    Status start();

    Status schedule(PipelineTimerTask* task, const timespec& abstime);
    //   0   -  Removed the task which does not run yet
    //  -1   -  The task does not exist.
    //   1   -  The task is just running.
    int unschedule(PipelineTimerTask* task);

    Status schedule(LightTimerTask* task, const timespec& abstime);

    int unschedule(LightTimerTask* task);

private:
    std::shared_ptr<bthread::TimerThread> _thr;
};
} // namespace starrocks::pipeline