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

// Test: force_put a task with wg1, take it back.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_single_workgroup) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, NUM_WORKERS);

    queue.force_put(make_wg_task(_wg1, 5));
    ASSERT_EQ(queue.size(), 1);

    // take(worker_id) is blocking but queue has 1 item, so it returns immediately.
    auto result = queue.take(0);
    ASSERT_TRUE(result.ok());
    ASSERT_NE(result.value().work_function, nullptr);
    ASSERT_EQ(queue.size(), 0);
}

// Test: consumer thread blocks on take, producer puts after a short delay,
// consumer should unblock and receive the task.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_blocking_wakeup) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, NUM_WORKERS);

    std::atomic<bool> consumer_got_task{false};

    auto consumer_thread = std::thread([&]() {
        auto result = queue.take(0);
        if (result.ok()) {
            consumer_got_task.store(true, std::memory_order_release);
        }
    });

    // Wait a bit to ensure the consumer is blocked.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(consumer_got_task.load(std::memory_order_acquire));

    // Put a task; the consumer should unblock.
    queue.force_put(make_wg_task(_wg1, 5));

    consumer_thread.join();
    ASSERT_TRUE(consumer_got_task.load(std::memory_order_acquire));
}

// Test: consumer blocks on take, close() wakes it and take returns Cancelled.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_close) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, NUM_WORKERS);

    std::atomic<bool> consumer_returned{false};
    bool take_ok = true;

    auto consumer_thread = std::thread([&]() {
        auto result = queue.take(0);
        take_ok = result.ok();
        consumer_returned.store(true, std::memory_order_release);
    });

    // Wait a bit to ensure the consumer is blocked.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(consumer_returned.load(std::memory_order_acquire));

    // Close the queue; the consumer should wake up and return Cancelled.
    queue.close();

    consumer_thread.join();
    ASSERT_TRUE(consumer_returned.load(std::memory_order_acquire));
    ASSERT_FALSE(take_ok);
}

// Test: take(worker_id) falls back to non-tokenized path for out-of-range worker_id.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_take_with_invalid_worker_id_fallback) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, NUM_WORKERS);

    queue.force_put(make_wg_task(_wg1, 5));
    auto result = queue.take(NUM_WORKERS);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value().workgroup, _wg1);
    ASSERT_EQ(queue.size(), 0);
}

// Test: try_offer enqueues a task that can be taken back.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_try_offer) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, NUM_WORKERS);

    ASSERT_TRUE(queue.try_offer(make_wg_task(_wg1, 10)));
    ASSERT_EQ(queue.size(), 1);

    auto result = queue.take(1);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(queue.size(), 0);
}

// Test: two workgroups with different vruntimes. The workgroup with lower
// vruntime should be selected first.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_workgroup_vruntime_selection) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, NUM_WORKERS);

    // Inflate wg2's vruntime so wg1 should be picked first.
    auto* entity2 = const_cast<WorkGroupScanSchedEntity*>(_wg2->scan_sched_entity());
    entity2->incr_runtime_ns(1'000'000'000L);

    queue.force_put(make_wg_task(_wg2, 5));
    queue.force_put(make_wg_task(_wg1, 5));

    // wg1 has lower vruntime, so its task should come out first.
    auto r1 = queue.take(0);
    ASSERT_TRUE(r1.ok());
    ASSERT_EQ(r1.value().workgroup, _wg1);

    auto r2 = queue.take(1);
    ASSERT_TRUE(r2.ok());
    ASSERT_EQ(r2.value().workgroup, _wg2);

    ASSERT_EQ(queue.size(), 0);
}

// Test: update_statistics increments workgroup vruntime.
TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_update_statistics) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, NUM_WORKERS);

    ScanTask task = make_wg_task(_wg1, 5);

    int64_t vruntime_before = _wg1->scan_sched_entity()->vruntime_ns();
    queue.update_statistics(task, 500'000'000L);
    int64_t vruntime_after = _wg1->scan_sched_entity()->vruntime_ns();

    ASSERT_GT(vruntime_after, vruntime_before);
}

} // namespace starrocks::workgroup
