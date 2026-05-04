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
#include <string>

#include "common/status.h"

namespace bthread {
class TimerThread;
}

namespace starrocks {

using BthreadTimerTaskId = int64_t;

class BthreadTimer;

class BthreadLightTimerTask {
public:
    virtual ~BthreadLightTimerTask() = default;

    virtual void Run() = 0;

    void set_tid(BthreadTimerTaskId tid) { _tid = tid; }
    BthreadTimerTaskId tid() const { return _tid; }

private:
    BthreadTimerTaskId _tid{};
};

class BthreadTimerTask : public std::enable_shared_from_this<BthreadTimerTask> {
public:
    virtual ~BthreadTimerTask() = default;

    void doRun() {
        auto self = shared_from_this();
        Run();
        _latch.count_down();
    }

    void unschedule(BthreadTimer* timer);

    void set_tid(BthreadTimerTaskId tid) { _tid = tid; }
    BthreadTimerTaskId tid() const { return _tid; }

private:
    // Only call when unschedule returns that the task is running.
    void waitUtilFinished();

protected:
    virtual void Run() = 0;

protected:
    std::latch _latch{1};
    BthreadTimerTaskId _tid{};
};

class BthreadTimer {
public:
    explicit BthreadTimer(std::string bvar_prefix = "bthread_timer");
    virtual ~BthreadTimer() noexcept = default;

    Status start();

    Status schedule(BthreadTimerTask* task, const timespec& abstime);
    //   0   -  Removed the task which does not run yet
    //  -1   -  The task does not exist.
    //   1   -  The task is just running.
    int unschedule(BthreadTimerTask* task);

    Status schedule(BthreadLightTimerTask* task, const timespec& abstime);

    int unschedule(BthreadLightTimerTask* task);

private:
    std::string _bvar_prefix;
    std::shared_ptr<bthread::TimerThread> _thr;
};

} // namespace starrocks
