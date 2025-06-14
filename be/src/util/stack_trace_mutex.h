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

#include <bthread/bthread.h>
#include <bthread/mutex.h>
#include <butil/time.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <ratio>
#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "gutil//macros.h"
#include "util/stack_util.h"
#include "util/time.h"

namespace starrocks {

extern void save_stack_trace_of_long_wait_mutex(const std::string& stack_trace);
extern std::vector<std::string> list_stack_trace_of_long_wait_mutex();

// If the lock() method failed to acquire the mutex for a long time(5 minutes), print a
// log message with call stack. Mainly used to detect dead lock in production environments.
// Example output:
// I0426 21:05:30.956799 22284 stack_trace_mutex.h:78] Long wait mutex:
//    @          0x5d760ca  starrocks::lake::AsyncDeltaWriter::open()
//    @          0x5d72a36  starrocks::LakeTabletsChannel::add_chunk()
//    @          0x5cc0f1f  starrocks::LoadChannel::_add_chunk()
//    @          0x5cc2165  starrocks::LoadChannel::add_chunk()
//    @          0x5cba902  starrocks::LoadChannelMgr::add_chunk()
//    @          0x5dd7345  starrocks::BackendInternalServiceImpl<>::tablet_writer_add_chunk()
//    @          0x721cb8d  brpc::policy::ProcessRpcRequest()
//    @          0x71930b7  brpc::ProcessInputMessage()
//    @          0x7193f8b  brpc::InputMessenger::OnNewMessages()
//    @          0x714446e  brpc::Socket::ProcessEvent()
//    @          0x7106e8f  bthread::TaskGroup::task_runner()
//    @          0x710b5c1  bthread_make_fcontext
template <typename Mutex>
class StackTraceMutex {
public:
    StackTraceMutex() : _mutex() {}

    DISALLOW_COPY_AND_MOVE(StackTraceMutex);

    void lock() {
        while (!try_lock_for(std::chrono::minutes(5))) {
            auto trace = get_stack_trace();
            save_stack_trace_of_long_wait_mutex(trace);
            LOG(INFO) << "Long wait mutex:\n" << trace;
        }
    }

    bool try_lock() { return _mutex.try_lock(); }

    template <class Rep, class Period>
    bool try_lock_for(const std::chrono::duration<Rep, Period>& timeout_duration) {
        return _mutex.try_lock_for(timeout_duration);
    }

    template <class Clock, class Duration>
    bool try_lock_until(const std::chrono::time_point<Clock, Duration>& timeout_time) {
        return _mutex.try_lock_until(timeout_time);
    }

    void unlock() { _mutex.unlock(); }

    Mutex& native_mutex() { return _mutex; }

private:
    Mutex _mutex;
};

// Specialize for bthread_mutex_t

template <>
class StackTraceMutex<bthread::Mutex> {
public:
    StackTraceMutex() : _mutex() {}

    void lock() {
        while (!try_lock_for(std::chrono::minutes(5))) {
            auto trace = get_stack_trace();
            save_stack_trace_of_long_wait_mutex(trace);
            LOG(INFO) << "Long wait mutex:\n" << trace;
        }
    }

    bool try_lock() { return _mutex.try_lock(); }

    template <class Rep, class Period>
    bool try_lock_for(const std::chrono::duration<Rep, Period>& timeout_duration) {
        auto rt = std::chrono::duration_cast<std::chrono::system_clock::duration>(timeout_duration);
        if (std::ratio_greater<std::chrono::system_clock::period, Period>()) ++rt;
        return try_lock_until(std::chrono::system_clock::now() + rt);
    }

    template <class Duration>
    bool try_lock_until(const std::chrono::time_point<std::chrono::system_clock, Duration>& timeout_time) {
        ::timespec due_time = TimespecFromTimePoint(timeout_time);
        return bthread_mutex_timedlock(_mutex.native_handler(), &due_time) == 0;
    }

    template <class Duration>
    bool try_lock_until(const std::chrono::time_point<std::chrono::steady_clock, Duration>& timeout_time) {
        ::timespec due_time = TimespecFromTimePoint(timeout_time);
        return bthread_mutex_timedlock(_mutex.native_handler(), &due_time) == 0;
    }

    template <class Clock, class Duration>
    bool try_lock_until(const std::chrono::time_point<Clock, Duration>& atime) {
        // The user-supplied clock may not tick at the same rate as
        // steady_clock, so we must loop in order to guarantee that
        // the timeout has expired before returning false.
        auto now = Clock::now();
        do {
            auto rtime = atime - now;
            if (try_lock_for(rtime)) return true;
            now = Clock::now();
        } while (atime > now);
        return false;
    }

    void unlock() { _mutex.unlock(); }

    bthread::Mutex& native_mutex() { return _mutex; }

private:
    bthread::Mutex _mutex;
};

} // namespace starrocks
