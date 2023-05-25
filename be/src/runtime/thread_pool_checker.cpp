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

#include "runtime/thread_pool_checker.h"

#include "common/config.h"

namespace starrocks {

Status ThreadPoolChecker::register_thread_pool(std::string thread_pool_name, ThreadPool* thread_pool) {
    LOG(INFO) << "register_thread_pool " << thread_pool_name;
    std::lock_guard lg(_thread_pool_checker_mutex);
    if (_thread_pool_holder.find(thread_pool_name) != _thread_pool_holder.end()) {
        std::stringstream msg;
        msg << "thread pool name " << thread_pool_name << " has been registered";
        LOG(WARNING) << msg.str();
        return Status::InternalError(msg.str());
    }
    _thread_pool_holder.emplace(thread_pool_name, thread_pool);
    return Status::OK();
}

Status ThreadPoolChecker::getStatus() {
    std::lock_guard lg(_thread_pool_checker_mutex);
    if (!_thread_pool_busy.empty()) {
        return Status::InternalError("threaPool is busy");
    }
    return Status::OK();
}

void ThreadPoolChecker::_collect_thread_pool_state() {
    std::lock_guard lg(_thread_pool_checker_mutex);
    for (const auto& iter : _thread_pool_holder) {

        if (iter.second == nullptr) {
            // it should't happen
            LOG(INFO) << "nullptr _collect_thread_pool_state" << iter.first;
            continue ;
        }
        int num_threads = iter.second->num_threads();
        int num_queued_tasks = iter.second->num_queued_tasks();
        int num_active_threads = iter.second->num_active_threads();
        LOG(INFO) << iter.first << " num_threads " << num_threads << " num_queued_task " << num_queued_tasks
                  << "num_active_threads " << num_active_threads;
        if (num_queued_tasks >= 10) {
            _thread_pool_busy.emplace(iter);
        }
    }
}

void* ThreadPoolChecker::_thread_pool_checker_callback(void* arg_this) {
    LOG(INFO) << "ThreadPoolChecker start working.";
    auto* thread_pool_checker_this = (ThreadPoolChecker*)arg_this;
    int32_t interval = config::health_check_interval;

    while (!thread_pool_checker_this->_stop) {
        thread_pool_checker_this->_collect_thread_pool_state();

        if (interval <= 0) {
            LOG(WARNING) << "threadPool check interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        int32_t left_seconds = interval;
        while (!thread_pool_checker_this->_stop.load(std::memory_order_consume) && left_seconds > 0) {
            sleep(1);
            --left_seconds;
        }
    }
    LOG(INFO) << "ThreadPoolChecker going to exit.";
    return nullptr;
}


void ThreadPoolChecker::debug(std::stringstream& ss) {
    std::lock_guard lg(_thread_pool_checker_mutex);
    for (const auto& iter : _thread_pool_holder) {
        if (iter.second == nullptr) {
            // it should't happen
            LOG(INFO) << "nullptr in threadPoolChecker debug" << iter.first;
            continue ;
        }
        int num_threads = iter.second->num_threads();
        int num_queued_tasks = iter.second->num_queued_tasks();
        int num_active_threads = iter.second->num_active_threads();
        ss << iter.first << " num_threads " << num_threads << " num_queued_task " << num_queued_tasks
                  << "num_active_threads " << num_active_threads;
        LOG(INFO) << ss.str();
    }
}


ThreadPoolChecker::ThreadPoolChecker() : BaseMonitor("thread_pool_checker") {
    _callback_function = _thread_pool_checker_callback;
}

ThreadPoolChecker::~ThreadPoolChecker() {}

} // namespace starrocks
