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

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>

#include "common/status.h"

namespace starrocks {

class ThreadPool;

class StorageCleanupExecutor {
public:
    StorageCleanupExecutor() = default;
    ~StorageCleanupExecutor();

    Status init();
    Status submit(std::function<void()> task);
    std::future<Status> submit_callable(std::function<Status()> task);
    void shutdown(int64_t drain_timeout_ms = 0);
    void wait();

    Status update_max_threads();
    ThreadPool* thread_pool() { return _thread_pool.get(); }

private:
    bool _begin_pool_op();
    void _finish_pool_op();

    // The mutex only protects the wrapper lifecycle state. A submit/update
    // operation increments this counter, leaves the lock, calls into the
    // underlying ThreadPool API, then decrements. The underlying pool can only
    // report tasks after they have entered the pool; this counter covers the
    // gap between passing the shutdown gate and entering ThreadPool::submit().
    // Shutdown flips the gate, waits for active pool ops, then gives accepted
    // tasks a bounded drain window before calling ThreadPool::shutdown(), which
    // may cancel queued work.
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _shutting_down = false;
    int64_t _active_pool_ops = 0;
    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace starrocks
