#include "io_dispatcher.h"

namespace starrocks::workgroup {

IoDispatcher::IoDispatcher(std::unique_ptr<ThreadPool> thread_pool) : _thread_pool(std::move(thread_pool)) {}

void IoDispatcher::initialize(int num_threads) {
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->run(); });
    }
}

void IoDispatcher::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->run(); });
    }
}

void IoDispatcher::run() {
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        auto wg = WorkGroupManager::instance()->pick_next_wg_for_io();
        if (wg == nullptr) {
            continue;
        }

        wg->pick_and_run_io_task();
    }
}

} // namespace starrocks::workgroup
