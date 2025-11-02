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
#include "butil/resource_pool.h"
#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/Types_types.h"
#include "runtime/current_thread.h"
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
    struct TraceContext {
        TUniqueId query_id;
        TUniqueId fragment_instance_id;
        int lwp_id = 0;
    };

    using TaskId = bthread::TimerThread::TaskId;
    SignalTimerGuard(int64_t timeout_ms) {
        if (timeout_ms > 0) {
            auto& current_thread = CurrentThread::current();
            // init trace context
            auto* trace = butil::get_resource(&_trace_context_id);
            static_assert(sizeof(uint64_t) == sizeof(_trace_context_id.value));

            trace->query_id = current_thread.query_id();
            trace->fragment_instance_id = current_thread.fragment_instance_id();
            trace->lwp_id = current_thread.get_lwp_id();

            auto timer_thread = bthread::get_global_timer_thread();
            timespec tm = butil::milliseconds_from_now(timeout_ms);
            // assign the trace context id to arg
            void* arg = nullptr;
            unaligned_store<uint64_t>(&arg, _trace_context_id.value);
            _tid = timer_thread->schedule(dump_trace_info, arg, tm);
        }
    }

    ~SignalTimerGuard() {
        if (_tid != bthread::TimerThread::INVALID_TASK_ID) {
            auto timer_thread = bthread::get_global_timer_thread();
            int res = timer_thread->unschedule(_tid);
            // return resource if timer not triggered
            if (res != 0) {
                butil::return_resource(_trace_context_id);
            }
        }
    }

private:
    butil::ResourceId<TraceContext> _trace_context_id;
    TaskId _tid = bthread::TimerThread::INVALID_TASK_ID;

    static void dump_trace_info(void* arg) {
        uint64_t trace_id = unaligned_load<uint64_t>(&arg);
        butil::ResourceId<TraceContext> trace_context_id{trace_id};
        auto* trace = butil::address_resource(trace_context_id);
        if (trace != nullptr) {
            std::string msg = get_stack_trace_for_thread(trace->lwp_id, 1000);
            LOG(INFO) << "query_id=" << print_id(trace->query_id)
                      << ", fragment_instance_id=" << print_id(trace->fragment_instance_id)
                      << ", found slow function:" << msg;
            butil::return_resource(trace_context_id);
        } else {
            LOG(INFO) << "can not address resource for trace context id:" << trace_id;
            DCHECK(false) << "can not found trace context";
        }
    }
};

}; // namespace starrocks

#define WARN_IF_TIMEOUT_MS(timeout_ms, msg_callback) \
    auto VARNAME_LINENUM(once) = TimeGuard(__FILE__, __LINE__, timeout_ms, msg_callback)

#define WARN_IF_TIMEOUT(timeout_ms, lazy_msg) WARN_IF_TIMEOUT_MS(timeout_ms, [&]() { return lazy_msg; })

#define WARN_IF_POLLER_TIMEOUT(lazy_msg) WARN_IF_TIMEOUT(config::pipeline_poller_timeout_guard_ms, lazy_msg)

#define DUMP_TRACE_IF_TIMEOUT(timeout_ms) auto VARNAME_LINENUM(guard) = SignalTimerGuard(timeout_ms)