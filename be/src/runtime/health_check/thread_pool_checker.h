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
#include <unordered_map>

#include "base_monitor.h"
#include "util/thread.h"
#include "util/threadpool.h"

namespace starrocks {

class ThreadPoolChecker : public BaseMonitor {
public:
    ThreadPoolChecker();
    ~ThreadPoolChecker() override;

    Status register_thread_pool(std::string name, ThreadPool* thread_pool);

    // monitor iface
    void debug(std::stringstream& ss) override;

private:
    static void* _thread_pool_checker_callback(void* arg_this);
    void _collect_thread_pool_state();

    std::mutex _thread_pool_checker_mutex;

    std::unordered_map<std::string, ThreadPool*> _thread_pool_holder;
    std::unordered_map<std::string, ThreadPool*> _thread_pool_busy;
};

} // namespace starrocks
