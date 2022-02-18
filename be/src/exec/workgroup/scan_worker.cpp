// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "scan_worker.h"

namespace starrocks::workgroup {

ScanWorker::ScanWorker(std::unique_ptr<ThreadPool> thread_pool) : _thread_pool(std::move(thread_pool)) {}

ScanWorker::~ScanWorker() {
    workgroup::WorkGroupManager::instance()->close();
}

void ScanWorker::initialize(int num_threads) {
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->run(); });
    }
}

void ScanWorker::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->run(); });
    }
}

void ScanWorker::run() {
    const int dispatcher_id = _next_id++;
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        auto maybe_task = WorkGroupManager::instance()->pick_next_task_for_io(dispatcher_id);
        if (maybe_task.status().is_cancelled()) {
            return;
        }

        maybe_task.value()(dispatcher_id);
    }
}

} // namespace starrocks::workgroup
