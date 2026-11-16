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

#include "udf/python/env.h"

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <memory>

#include "common/config.h"
#include "util/defer_op.h"

namespace starrocks {

// A pooled worker idle past python_worker_expire_time_sec is reclaimed, but not while a call stub is
// still connected to it: a query that keeps calling the UDF for longer than the expire time would
// otherwise lose its worker mid-query and fail with "Socket closed".
TEST(PyWorkerManagerEnvTest, worker_in_use_is_not_expired) {
    // A real child stands in for the Python worker, so terminate_and_wait has a process to kill and reap.
    pid_t pid = fork();
    ASSERT_NE(-1, pid);
    if (pid == 0) {
        pause();
        _exit(0);
    }

    auto saved_expire_time_sec = config::python_worker_expire_time_sec;
    // Every unused worker counts as expired.
    config::python_worker_expire_time_sec = -1;
    auto& manager = PyWorkerManager::getInstance();
    constexpr int32_t driver_id = 0x7fff0001;
    DeferOp restore([&]() {
        config::python_worker_expire_time_sec = saved_expire_time_sec;
        std::lock_guard guard(manager._mutex);
        manager._processes.erase(driver_id);
    });
    auto pooled = [&]() {
        std::lock_guard guard(manager._mutex);
        return manager._processes[driver_id].size();
    };

    auto worker = std::make_shared<PyWorker>(pid);
    worker->touch();
    worker->add_client();
    {
        std::lock_guard guard(manager._mutex);
        manager._processes[driver_id].push_back(worker);
    }
    ASSERT_EQ(1, worker->num_clients());
    manager.cleanup_expired_worker();
    ASSERT_EQ(1, pooled());
    ASSERT_EQ(0, kill(pid, 0)) << "a worker in use must not be terminated";

    worker->remove_client();
    manager.cleanup_expired_worker();
    ASSERT_EQ(0, pooled());
    ASSERT_EQ(-1, kill(pid, 0)) << "an expired worker must be terminated and reaped";
}

} // namespace starrocks
