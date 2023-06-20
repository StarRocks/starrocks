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

#include "exec/workgroup/scan_executor.h"

#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "util/starrocks_metrics.h"

namespace starrocks::workgroup {

ScanExecutor::ScanExecutor(std::unique_ptr<ThreadPool> thread_pool, std::unique_ptr<ScanTaskQueue> task_queue)
        : _task_queue(std::move(task_queue)), _thread_pool(std::move(thread_pool)) {
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_scan_executor_queuing, [this]() { return _task_queue->size(); });
}

ScanExecutor::~ScanExecutor() {
    _task_queue->close();
}

void ScanExecutor::initialize(int num_threads) {
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        (void)_thread_pool->submit_func([this]() { this->worker_thread(); });
    }
}

void ScanExecutor::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        (void)_thread_pool->submit_func([this]() { this->worker_thread(); });
    }
}

void ScanExecutor::worker_thread() {
    auto current_thread = Thread::current_thread();
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        if (current_thread != nullptr) {
            current_thread->set_idle(true);
        }
        auto maybe_task = _task_queue->take();
        if (current_thread != nullptr) {
            current_thread->set_idle(false);
        }
        if (maybe_task.status().is_cancelled()) {
            return;
        }
        auto& task = maybe_task.value();

        int64_t time_spent_ns = 0;
        {
            SCOPED_RAW_TIMER(&time_spent_ns);
            task.work_function();
        }
        if (current_thread != nullptr) {
            current_thread->inc_finished_tasks();
        }
        _task_queue->update_statistics(task, time_spent_ns);
    }
}

bool ScanExecutor::submit(ScanTask task) {
    return _task_queue->try_offer(std::move(task));
}

int ScanExecutor::submit(void* (*fn)(void*), void* args) {
    // TODO: specify the workgroup through parameter
    auto wg = WorkGroupManager::instance()->get_default_mv_workgroup();
    ScanTask::WorkFunction wf = [=]() { fn(args); };
    ScanTask task(wg.get(), std::move(wf));
    return _task_queue->try_offer(std::move(task)) ? 0 : 1;
}

bool ScanExecutor::submit(std::function<void()> fun) {
    // TODO: specify the workgroup through parameter
    auto wg = WorkGroupManager::instance()->get_default_mv_workgroup();
    ScanTask task(wg.get(), std::move(fun));
    return _task_queue->try_offer(std::move(task));
}

bool ScanExecutor::submit_urgent(void* (*fn)(void*), void* args) {
    return _thread_pool->submit_func([=]() { fn(args); }, ThreadPool::Priority::HIGH_PRIORITY).ok();
}

std::unique_ptr<TaskToken> ScanExecutor::new_token(const std::string& name) {
    return std::make_unique<ExecutorToken>(name, this);
}

ExecutorToken::~ExecutorToken() {
    close();
}

void* ExecutorToken::_worker(void* args) {
    // A worker could only run for at most 100ms, to avoid occupy too much resource
    constexpr int64_t kWorkerTimeSliceNs = 100'000'000;

    auto* token = static_cast<ExecutorToken*>(args);

    // If there's already a worker, we should quit
    {
        std::unique_lock<std::mutex> lock(token->_mutex);
        if (token->_state == RUNNING) {
            return nullptr;
        } else {
            token->_state = RUNNING;
        }
    }

    // set name of this thread
    Thread* current = Thread::current_thread();
    std::string prev_name = current->name();
    DeferOp defer([&]() { Thread::set_thread_name(current->pthread_id(), prev_name); });
    Thread::set_thread_name(current->pthread_id(), token->_name);
    token->_executed_workers++;

    int64_t total_task_ns = 0;
    while (true) {
        TaskT task;
        {
            std::unique_lock<std::mutex> lock(token->_mutex);
            DCHECK_EQ(token->_state, RUNNING);
            task = token->_take_task();
            if (task == nullptr) {
                token->_state = IDLE;
                token->_cond.notify_all();
                VLOG(10) << "no more tasks, ExecutorToken worker quit";
                return nullptr;
            }
        }
        int64_t task_ns = 0;
        {
            DCHECK(!!task);
            SCOPED_RAW_TIMER(&task_ns);
            task->run();
        }
        total_task_ns += task_ns;

        token->_executed_tasks++;
        token->_executed_time_ns += task_ns;

        // Only if no one is waiting, and use too much time slice, the worker could exit
        std::unique_lock<std::mutex> lock(token->_mutex);
        if (token->_num_waiters == 0 && total_task_ns > kWorkerTimeSliceNs) {
            break;
        }
    }
    VLOG(10) << "ExecutorToken run " << total_task_ns << "ns, need to quit ";

    // Submit a new worker, and change my state = STAGING
    std::unique_lock<std::mutex> lock(token->_mutex);
    token->_executor->submit(_worker, token);
    token->_state = STAGING;
    return nullptr;
}

Status ExecutorToken::submit(TaskT task) {
    std::unique_lock<std::mutex> lock(_mutex);
    _tasks.push_back(task);
    if (_state == IDLE) {
        _state = STAGING;
        _executor->submit(_worker, this);
    }

    return {};
}

Status ExecutorToken::submit(std::function<void(void)> fun) {
    return submit(FunctionRunnable::make_task(std::move(fun)));
}

ExecutorToken::TaskT ExecutorToken::_take_task() {
    if (!_tasks.empty()) {
        TaskT res = std::move(_tasks.front());
        _tasks.pop_front();
        return res;
    }
    return nullptr;
}

void ExecutorToken::close() {
    {
        std::unique_lock<std::mutex> lock(_mutex);
        VLOG(10) << "destroy queuing tasks: " << _tasks.size();
        _tasks.clear();
    }

    wait();
}

// If there's already a worker running in the threadpool, just waiting for its finishing
// If not, the waiting may introduce deadlock, because maybe no worker will be able to drain tasks
// So we need to check whether there' a worker running:
// 1. Change the state to WATIING, to let the worker know someone is waiting for it,
//    and the worker should not yield voluntarily
// 2. If some worker is running, the waiter just need to wait the condition
// 3. If no worker, the waiter need to submit an urgent worker, to break the deadlock
void ExecutorToken::wait() {
    std::unique_lock<std::mutex> lock(_mutex);
    _num_waiters++;
    switch (_state) {
    case IDLE:
        break;
    case STAGING: {
        VLOG(10) << "no worker is running, submit urgent worker";
        _executor->submit_urgent(_worker, this);
        _cond.wait(lock, [&]() { return _state == IDLE; });
        break;
    }
    case RUNNING: {
        _cond.wait(lock, [&]() { return _state == IDLE; });
        break;
    }
    default:
        CHECK(false);
    }
    _num_waiters--;
}

ThreadPoolTaskToken::ThreadPoolTaskToken(std::unique_ptr<ThreadPoolToken> pool_token)
        : _pool_token(std::move(pool_token)) {}

Status ThreadPoolTaskToken::submit(TaskT task) {
    return _pool_token->submit(task);
}

Status ThreadPoolTaskToken::submit(std::function<void()> task) {
    return _pool_token->submit_func(task);
}

void ThreadPoolTaskToken::close() {
    _pool_token->shutdown();
}

void ThreadPoolTaskToken::wait() {
    _pool_token->wait();
}

} // namespace starrocks::workgroup
