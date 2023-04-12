// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/workgroup/scan_executor.h"

#include "exec/workgroup/scan_task_queue.h"
#include "runtime/exec_env.h"

namespace starrocks::workgroup {

ScanExecutor::ScanExecutor(std::unique_ptr<ThreadPool> thread_pool, std::unique_ptr<ScanTaskQueue> task_queue)
        : _task_queue(std::move(task_queue)), _thread_pool(std::move(thread_pool)) {}

ScanExecutor::~ScanExecutor() {
    _task_queue->close();
}

void ScanExecutor::initialize(int num_threads) {
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->worker_thread(); });
    }
}

void ScanExecutor::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->worker_thread(); });
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
        _task_queue->update_statistics(task.workgroup, time_spent_ns);
    }
}

bool ScanExecutor::submit(ScanTask task) {
    return _task_queue->try_offer(std::move(task));
}

} // namespace starrocks::workgroup
