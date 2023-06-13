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

Status ScanExecutor::submit(std::shared_ptr<Runnable> r, ThreadPool::Priority pri) {
    auto wg = WorkGroupManager::instance()->get_default_mv_workgroup();
    ScanTask task(wg.get(), [=]() { r->run(); });
    return _task_queue->try_offer(std::move(task)) ? Status::OK() : Status::ResourceBusy("enqueue failed");
}

std::unique_ptr<ThreadPoolToken> ScanExecutor::new_token(ThreadPool::ExecutionMode mode) {
    // TODO: should not bypass the task queue
    return _thread_pool->new_token(mode);
}

} // namespace starrocks::workgroup
