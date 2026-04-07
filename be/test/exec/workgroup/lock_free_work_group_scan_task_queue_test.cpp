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

#include "exec/workgroup/lock_free_work_group_scan_task_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::workgroup {

static ScanTask make_wg_task(WorkGroupPtr wg, int priority) {
    ScanTask task([](YieldContext&) {});
    task.priority = priority;
    task.workgroup = std::move(wg);
    return task;
}

class LockFreeWorkGroupScanTaskQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        _wg1 = std::make_shared<WorkGroup>("scan_wg1", 10, WorkGroup::DEFAULT_VERSION, 1, 0.5, 10, 1.0,
                                           WorkGroupType::WG_NORMAL, WorkGroup::DEFAULT_MEM_POOL);
        _wg2 = std::make_shared<WorkGroup>("scan_wg2", 20, WorkGroup::DEFAULT_VERSION, 2, 0.5, 10, 1.0,
                                           WorkGroupType::WG_NORMAL, WorkGroup::DEFAULT_MEM_POOL);
        _wg1 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg1);
        _wg2 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg2);
    }

protected:
    WorkGroupPtr _wg1;
    WorkGroupPtr _wg2;
};

// Test: force_put a task with wg1, take it back non-blocking.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_single_workgroup) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(NUM_WORKERS);

    queue.force_put(make_wg_task(_wg1, 5), 0);
    ASSERT_EQ(queue.size(), 1);

    ScanTask out;
    ASSERT_TRUE(queue.take(out, /*blocking=*/false));
    ASSERT_NE(out.work_function, nullptr);
    ASSERT_EQ(queue.size(), 0);
}

// Test: consumer thread blocks on take, producer puts after a short delay,
// consumer should unblock and receive the task.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_blocking_wakeup) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(NUM_WORKERS);

    std::atomic<bool> consumer_got_task{false};

    auto consumer_thread = std::thread([&]() {
        ScanTask out;
        bool ok = queue.take(out, /*blocking=*/true);
        if (ok) {
            consumer_got_task.store(true, std::memory_order_release);
        }
    });

    // Wait a bit to ensure the consumer is blocked.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(consumer_got_task.load(std::memory_order_acquire));

    // Put a task; the consumer should unblock.
    queue.force_put(make_wg_task(_wg1, 5), 0);

    consumer_thread.join();
    ASSERT_TRUE(consumer_got_task.load(std::memory_order_acquire));
}

// Test: consumer blocks on take, close() wakes it and take returns false.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_close) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(NUM_WORKERS);

    std::atomic<bool> consumer_returned{false};
    bool take_result = true;

    auto consumer_thread = std::thread([&]() {
        ScanTask out;
        take_result = queue.take(out, /*blocking=*/true);
        consumer_returned.store(true, std::memory_order_release);
    });

    // Wait a bit to ensure the consumer is blocked.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(consumer_returned.load(std::memory_order_acquire));

    // Close the queue; the consumer should wake up and return false.
    queue.close();

    consumer_thread.join();
    ASSERT_TRUE(consumer_returned.load(std::memory_order_acquire));
    ASSERT_FALSE(take_result);
}

// Test: non-blocking take on empty queue returns false.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_empty) {
    constexpr int NUM_WORKERS = 2;
    LockFreeWorkGroupScanTaskQueue queue(NUM_WORKERS);

    ScanTask out;
    ASSERT_FALSE(queue.take(out, /*blocking=*/false));
    ASSERT_EQ(queue.size(), 0);
}

} // namespace starrocks::workgroup
