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

#include "common/bthread_timer.h"

#include <memory>
#include <utility>

#include "bthread/timer_thread.h"
#include "fmt/format.h"

namespace starrocks {

void BthreadTimerTask::waitUtilFinished() {
    _latch.wait();
}

void BthreadTimerTask::unschedule_and_join(BthreadTimer* timer) {
    int rc = timer->unschedule(this);
    if (rc == 1) {
        waitUtilFinished();
    }
}

BthreadTimer::BthreadTimer(std::string bvar_prefix) : _bvar_prefix(std::move(bvar_prefix)) {}

Status BthreadTimer::start() {
    _thr = std::make_shared<bthread::TimerThread>();
    bthread::TimerThreadOptions options;
    options.bvar_prefix = _bvar_prefix;
    int rc = _thr->start(&options);
    if (rc != 0) {
        return Status::InternalError(fmt::format("init bthread timer error:{}", berror(errno)));
    }
    return Status::OK();
}

static void RunTimerTask(void* arg) {
    auto* task = static_cast<BthreadTimerTask*>(arg);
    task->doRun();
}

Status BthreadTimer::schedule(BthreadTimerTask* task, const timespec& abstime) {
    BthreadTimerTaskId tid = _thr->schedule(RunTimerTask, task, abstime);
    if (tid == 0) {
        return Status::InternalError(fmt::format("bthread timer schedule task error:{}", berror(errno)));
    }
    task->set_tid(tid);
    return Status::OK();
}

int BthreadTimer::unschedule(BthreadTimerTask* task) {
    return _thr->unschedule(task->tid());
}

static void RunLightTimerTask(void* arg) {
    auto* task = static_cast<BthreadLightTimerTask*>(arg);
    task->Run();
}

Status BthreadTimer::schedule(BthreadLightTimerTask* task, const timespec& abstime) {
    BthreadTimerTaskId tid = _thr->schedule(RunLightTimerTask, task, abstime);
    if (tid == 0) {
        return Status::InternalError(fmt::format("bthread timer schedule task error:{}", berror(errno)));
    }
    task->set_tid(tid);
    return Status::OK();
}

int BthreadTimer::unschedule(BthreadLightTimerTask* task) {
    return _thr->unschedule(task->tid());
}

} // namespace starrocks
