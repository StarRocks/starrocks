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

// Note: include order matters
// clang-format off
#include "common/compiler_util.h" // DIAGNOSTIC_PUSH

#include <bthread/execution_queue.h>

// clang-format on

#include <cassert>

#include "common/ownership.h"
#include "gutil/macros.h"

namespace starrocks {
class ThreadPool;
}

namespace starrocks::bthreads {

// Used to run bthread::ExecutionQueue task in pthread instead of bthread.
// Reference: https://github.com/apache/brpc/blob/master/docs/cn/execution_queue.md
class ThreadPoolExecutor : public bthread::Executor {
public:
    constexpr static int64_t kDefaultBusySleepMs = 50;

    ThreadPoolExecutor() : _thread_pool(nullptr), _ownership(kDontTakeOwnership), _busy_sleep_ms(kDefaultBusySleepMs) {}

    explicit ThreadPoolExecutor(ThreadPool* pool, Ownership ownership)
            : _thread_pool(pool), _ownership(ownership), _busy_sleep_ms(kDefaultBusySleepMs) {}

    DISALLOW_COPY_AND_MOVE(ThreadPoolExecutor);

    ~ThreadPoolExecutor() override;

    void set_thread_pool(ThreadPool* thread_pool) {
        assert(_thread_pool == nullptr);
        _thread_pool = thread_pool;
    }

    ThreadPool* get_thread_pool() { return _thread_pool; }

    void set_ownership(Ownership ownership) { _ownership = ownership; }

    void set_busy_sleep_ms(int64_t value) { _busy_sleep_ms = value; }

    int submit(void* (*fn)(void*), void* args) override;

private:
    ThreadPool* _thread_pool;
    Ownership _ownership;
    int64_t _busy_sleep_ms;
};

} // namespace starrocks::bthreads
