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

#include "runtime/monitor_manager.h"
#include "runtime/thread_pool_checker.h"
#include <gtest/gtest.h>

namespace starrocks {

class MonitorManagerTest : public testing::Test {
public:
    MonitorManagerTest() = default;
    ~MonitorManagerTest() override = default;

    void SetUp() override {

    }

    void TearDown() override {

    }

};

TEST_F(MonitorManagerTest, monitor_manager) {
    std::unique_ptr<MonitorManager> monitor_manager_ptr(new MonitorManager());
    std::unique_ptr<ThreadPoolChecker> thread_pool_checker(new ThreadPoolChecker());

    std::unique_ptr<ThreadPool> thread_pool_test;
    ThreadPoolBuilder("thread_pool_test")
                            .set_min_threads(0)
                            .set_max_threads(8)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&thread_pool_test);

    thread_pool_checker->register_thread_pool("thread_pool_test", thread_pool_test.get());

    monitor_manager_ptr->register_monitor(thread_pool_checker->get_name(), thread_pool_checker.get());
    monitor_manager_ptr->start_all_monitors();
    sleep(10);
    monitor_manager_ptr->stop();
}

} // namespace starrocks
