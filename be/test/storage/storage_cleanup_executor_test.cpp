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

#include "storage/storage_cleanup_executor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <thread>

#include "base/testutil/assert.h"
#include "common/config_agent_fwd.h"
#include "common/status.h"
#include "common/thread/threadpool.h"

namespace starrocks {

class StorageCleanupExecutorTest : public testing::Test {
protected:
    void SetUp() override {
        _old_drop_tablet_worker_count = config::drop_tablet_worker_count;
        config::drop_tablet_worker_count = 1;
    }

    void TearDown() override { config::drop_tablet_worker_count = _old_drop_tablet_worker_count; }

private:
    int32_t _old_drop_tablet_worker_count = 0;
};

TEST_F(StorageCleanupExecutorTest, SubmitAndCallable) {
    StorageCleanupExecutor executor;
    ASSERT_OK(executor.init());

    std::atomic<int> ran = 0;
    ASSERT_OK(executor.submit([&ran] { ++ran; }));
    auto future = executor.submit_callable([] { return Status::OK(); });

    executor.wait();
    EXPECT_EQ(1, ran.load());
    ASSERT_TRUE(future.valid());
    ASSERT_OK(future.get());
}

TEST_F(StorageCleanupExecutorTest, RejectsSubmitAfterShutdown) {
    StorageCleanupExecutor executor;
    ASSERT_OK(executor.init());

    executor.shutdown(1000);

    auto status = executor.submit([] {});
    EXPECT_TRUE(status.is_service_unavailable()) << status;
    auto future = executor.submit_callable([] { return Status::OK(); });
    ASSERT_TRUE(future.valid());
    EXPECT_TRUE(future.get().is_service_unavailable());
}

TEST_F(StorageCleanupExecutorTest, ShutdownDrainsAcceptedTasks) {
    StorageCleanupExecutor executor;
    ASSERT_OK(executor.init());

    std::promise<void> first_started_promise;
    auto first_started = first_started_promise.get_future();
    std::promise<void> release_first_promise;
    auto release_first = release_first_promise.get_future().share();
    std::atomic<int> ran = 0;

    ASSERT_OK(executor.submit([&] {
        first_started_promise.set_value();
        release_first.wait();
        ++ran;
    }));
    ASSERT_OK(executor.submit([&] { ++ran; }));

    first_started.wait();
    std::thread shutdown_thread([&executor] { executor.shutdown(5000); });
    release_first_promise.set_value();
    shutdown_thread.join();

    EXPECT_EQ(2, ran.load());
}

TEST_F(StorageCleanupExecutorTest, UpdateMaxThreadsUsesDropTabletWorkerCount) {
    StorageCleanupExecutor executor;
    ASSERT_OK(executor.init());
    ASSERT_EQ(1, executor.thread_pool()->max_threads());

    config::drop_tablet_worker_count = 3;
    ASSERT_OK(executor.update_max_threads());
    ASSERT_EQ(3, executor.thread_pool()->max_threads());
}

} // namespace starrocks
