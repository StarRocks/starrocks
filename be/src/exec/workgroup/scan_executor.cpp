// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "scan_executor.h"

namespace starrocks::workgroup {

ScanExecutor::ScanExecutor(std::unique_ptr<ThreadPool> thread_pool) : _thread_pool(std::move(thread_pool)) {}

ScanExecutor::~ScanExecutor() {
    workgroup::WorkGroupManager::instance()->close();
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
    const int worker_id = _next_id++;
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        auto maybe_task = WorkGroupManager::instance()->pick_next_task_for_io(worker_id);
        if (maybe_task.status().is_cancelled()) {
            return;
        }

        maybe_task.value()(worker_id);
    }
}

} // namespace starrocks::workgroup
