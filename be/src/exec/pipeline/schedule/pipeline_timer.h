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

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>

#include "common/status.h"

namespace bthread {
class TimerThread;
}

namespace starrocks::pipeline {
using TaskId = int64_t;
class PipelineTimer;

class PipelineTimerTask {
public:
    virtual ~PipelineTimerTask() = default;

    void doRun() {
        Run();
        _finished.store(true, std::memory_order_seq_cst);
        if (_has_consumer.load(std::memory_order_acquire)) {
            _cv.notify_one();
        }
    }

    // only call when unschedule == 1
    void waitUtilFinished();
    void unschedule(PipelineTimer* timer);

    void set_tid(TaskId tid) { _tid = tid; }
    TaskId tid() const { return _tid; }

protected:
    // implement interface
    virtual void Run() = 0;

protected:
    std::atomic<bool> _finished{};
    std::atomic<bool> _has_consumer{};
    TaskId _tid{};
    std::mutex _mutex;
    std::condition_variable _cv;
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

private:
    std::shared_ptr<bthread::TimerThread> _thr;
};
} // namespace starrocks::pipeline