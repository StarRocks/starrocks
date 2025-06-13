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
#include <csignal>
#include <thread>

#include "bthread/timer_thread.h"
#include "common/config.h"
#include "common/logging.h"
#include "util/time.h"
#include "util/unaligned_access.h"

namespace starrocks {

template <class LazyMsgCallBack>
class TimeGuard {
public:
    TimeGuard(const char* file_name, size_t line, int64_t timeout_ms, LazyMsgCallBack callback)
            : _file_name(file_name), _line(line), _timeout_ms(timeout_ms), _callback(callback) {
        if (_timeout_ms > 0) {
            _begin_time = MonotonicMillis();
        }
    }

    ~TimeGuard() {
        if (_timeout_ms > 0) {
            int64_t cost_ms = MonotonicMillis() - _begin_time;
            if (cost_ms > _timeout_ms) {
                LOG(WARNING) << _file_name << ":" << _line << " cost:" << cost_ms << " " << _callback();
            }
        }
    }

private:
    const char* _file_name;
    size_t _line;
    int64_t _timeout_ms;
    LazyMsgCallBack _callback;
    int64_t _begin_time = 0;
};

// export from stack_util.h
std::string get_stack_trace_for_thread(int tid, int timeout_ms);

// SignalTimerGuard class manages a timer to capture and log thread stack traces after a specified timeout.
// Note: If bthread yields. you may not get the expected stacktrace. But it's still safe.
// usage:
// {
//     SignalTimerGuard guard(100);
//     monitor_function();
// }
class SignalTimerGuard {
public:
    using TaskId = bthread::TimerThread::TaskId;
    SignalTimerGuard(int64_t timeout_ms) {
        if (timeout_ms > 0) {
            auto timer_thread = bthread::get_global_timer_thread();
            timespec tm = butil::milliseconds_from_now(timeout_ms);
            int lwp_id = syscall(SYS_gettid);
            void* arg = nullptr;
            unaligned_store<int>(&arg, lwp_id);
            _tid = timer_thread->schedule(dump_trace_info, arg, tm);
        }
    }

    ~SignalTimerGuard() {
        if (_tid != bthread::TimerThread::INVALID_TASK_ID) {
            auto timer_thread = bthread::get_global_timer_thread();
            timer_thread->unschedule(_tid);
        }
    }

private:
    TaskId _tid = bthread::TimerThread::INVALID_TASK_ID;

    static void dump_trace_info(void* arg) {
        int tid = unaligned_load<int>(&arg);
        std::string msg = get_stack_trace_for_thread(tid, 1000);
        LOG(INFO) << "found slow function:" << msg;
    }
};

}; // namespace starrocks

#define WARN_IF_TIMEOUT_MS(timeout_ms, msg_callback) \
    auto VARNAME_LINENUM(once) = TimeGuard(__FILE__, __LINE__, timeout_ms, msg_callback)

#define WARN_IF_TIMEOUT(timeout_ms, lazy_msg) WARN_IF_TIMEOUT_MS(timeout_ms, [&]() { return lazy_msg; })

#define WARN_IF_POLLER_TIMEOUT(lazy_msg) WARN_IF_TIMEOUT(config::pipeline_poller_timeout_guard_ms, lazy_msg)