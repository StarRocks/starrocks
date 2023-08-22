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

#include <bthread/execution_queue.h>

#include <utility>

#include "util/limit_setter.h"
#include "util/threadpool.h"
#include "work_group.h"

namespace starrocks::workgroup {

class ScanExecutor;
class WorkGroupManager;
struct ScanTask;
class ScanTaskQueue;
class TaskToken;

class ScanExecutor : public bthread::Executor {
public:
    explicit ScanExecutor(std::unique_ptr<ThreadPool> thread_pool, std::unique_ptr<ScanTaskQueue> task_queue);
    ~ScanExecutor() override;

    void initialize(int32_t num_threads);
    void change_num_threads(int32_t num_threads);

    bool submit(ScanTask task);

    // NOTE: can only used for low-priority tasks, like materialized view
    int submit(void* (*fn)(void*), void* args) override;
    bool submit(std::function<void()> fun);

    // submit urgent task to break the deadlock
    bool submit_urgent(void* (*fn)(void*), void* args);

    std::unique_ptr<TaskToken> new_token(const std::string& name);

private:
    void worker_thread();

    LimitSetter _num_threads_setter;
    std::unique_ptr<ScanTaskQueue> _task_queue;
    // _thread_pool must be placed after _task_queue, because worker threads in _thread_pool use _task_queue.
    std::unique_ptr<ThreadPool> _thread_pool;
    std::atomic<int> _next_id = 0;
};

// Execute group of tasks
class TaskToken {
public:
    using TaskT = std::shared_ptr<Runnable>;

    virtual ~TaskToken() = default;

    virtual Status submit(TaskT task) = 0;
    virtual Status submit(std::function<void(void)> task) = 0;
    Status submit_func(std::function<void()> task) { return submit(std::move(task)); }
    virtual void close() = 0;
    void shutdown() { close(); }
    virtual void wait() = 0;
};

// ExecutorToken is used to execute a group of tasks, and scheduled by ResourceGroup
// NOTE: it's a vanilla implementation, without concurrent execution, without priority control
// NOTE: the underlying executor has to be a ScanExecutor, or some kinds of schedulable Executor
class ExecutorToken final : public TaskToken {
public:
    enum State {
        IDLE,    // No worker
        STAGING, // Worker task has been submitted, but not running
        RUNNING, // Worker is running in the threadpool
    };

    ExecutorToken(std::string name, ScanExecutor* executor) : _name(std::move(name)), _executor(executor) {}
    ~ExecutorToken() override;

    // Submit a new task
    Status submit(TaskT task) override;
    Status submit(std::function<void(void)> task) override;

    // Destroy not-running tasks, and wait for running tasks
    void close() override;

    // Wait all submitted tasks
    void wait() override;

    State get_state() const {
        std::unique_lock<std::mutex> lock(_mutex);
        return _state;
    }

    int64_t executed_tasks() const { return _executed_tasks; }
    int64_t executed_time_ns() const { return _executed_time_ns; }
    int64_t executed_time_ms() const { return _executed_time_ns / 1'000'000; }
    int64_t executed_workers() const { return _executed_workers; }

private:
    class FunctionRunnable final : public Runnable {
    public:
        FunctionRunnable(std::function<void(void)> fun) : _fun(std::move(fun)) {}

        static TaskT make_task(std::function<void(void)> fun) {
            return std::make_shared<FunctionRunnable>(std::move(fun));
        }

        void run() override { _fun(); }

    private:
        std::function<void(void)> _fun;
    };

    TaskT _take_task();
    static void* _worker(void*);

    mutable std::mutex _mutex;
    std::string _name;
    std::condition_variable _cond;
    std::deque<TaskT> _tasks;
    ScanExecutor* _executor;
    State _state = IDLE;
    int _num_waiters = 0;

    // statistics
    std::atomic_int64_t _executed_tasks = 0;
    std::atomic_int64_t _executed_time_ns = 0;
    std::atomic_int64_t _executed_workers = 0;
};

// Wrap the bthread::ThreadPoolToken, implementjthe TaskToken interface
class ThreadPoolTaskToken final : public TaskToken {
public:
    ThreadPoolTaskToken(std::unique_ptr<ThreadPoolToken> pool_token);
    ~ThreadPoolTaskToken() override = default;

    Status submit(TaskT task) override;
    Status submit(std::function<void()> task) override;
    void close() override;
    void wait() override;

private:
    std::unique_ptr<ThreadPoolToken> _pool_token;
};

} // namespace starrocks::workgroup
