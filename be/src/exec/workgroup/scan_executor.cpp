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

#include "exec/pipeline/pipeline_metrics.h"
#include "exec/workgroup/scan_task_queue.h"
#include "util/starrocks_metrics.h"

namespace starrocks::workgroup {

ScanExecutor::ScanExecutor(std::unique_ptr<ThreadPool> thread_pool, std::unique_ptr<ScanTaskQueue> task_queue,
                           pipeline::ScanExecutorMetrics* metrics)
        : _task_queue(std::move(task_queue)), _thread_pool(std::move(thread_pool)), _metrics(metrics) {}

void ScanExecutor::close() {
    _task_queue->close();
    _thread_pool->shutdown();
    _metrics = nullptr;
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
        if (_num_threads_setter.should_expand()) {
            (void)_thread_pool->submit_func([this]() { this->worker_thread(); });
        }
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
        _metrics->pending_tasks.increment(-1);
        if (current_thread != nullptr) {
            current_thread->set_idle(false);
        }
        if (maybe_task.status().is_cancelled()) {
            return;
        }
        auto& task = maybe_task.value();

        _metrics->running_tasks.increment(1);
        int64_t time_spent_ns = 0;
        {
            SCOPED_RAW_TIMER(&time_spent_ns);
            task.run();
        }
        if (current_thread != nullptr) {
            current_thread->inc_finished_tasks();
        }
        _task_queue->update_statistics(task, time_spent_ns);
        _metrics->running_tasks.increment(-1);
        _metrics->finished_tasks.increment(1);
        _metrics->execution_time.increment(time_spent_ns);

        // task
        if (!task.is_finished()) {
            _task_queue->force_put(std::move(task));
        }
    }
}

bool ScanExecutor::submit(ScanTask task) {
    bool ret = _task_queue->try_offer(std::move(task));
    _metrics->pending_tasks.increment(ret);
    return ret;
}

void ScanExecutor::force_submit(ScanTask task) {
    _task_queue->force_put(std::move(task));
    _metrics->pending_tasks.increment(1);
}

void ScanExecutor::bind_cpus(const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) {
    _thread_pool->bind_cpus(cpuids, borrowed_cpuids);
}

int64_t ScanExecutor::num_tasks() const {
    return _task_queue->size();
}

} // namespace starrocks::workgroup
