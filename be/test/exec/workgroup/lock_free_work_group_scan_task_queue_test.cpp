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

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_single_workgroup) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    queue.force_put(make_wg_task(_wg1, 5));
    ASSERT_EQ(queue.size(), 1);

    auto result = queue.take(0);
    ASSERT_TRUE(result.ok());
    ASSERT_NE(result.value().work_function, nullptr);
    ASSERT_EQ(queue.size(), 0);
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_blocking_wakeup) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    std::atomic<bool> consumer_got_task{false};
    auto consumer_thread = std::thread([&]() {
        auto result = queue.take(0);
        if (result.ok()) {
            consumer_got_task.store(true, std::memory_order_release);
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(consumer_got_task.load(std::memory_order_acquire));

    queue.force_put(make_wg_task(_wg1, 5));

    consumer_thread.join();
    ASSERT_TRUE(consumer_got_task.load(std::memory_order_acquire));
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_close) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    std::atomic<bool> consumer_returned{false};
    bool take_ok = true;
    auto consumer_thread = std::thread([&]() {
        auto result = queue.take(0);
        take_ok = result.ok();
        consumer_returned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(consumer_returned.load(std::memory_order_acquire));

    queue.close();

    consumer_thread.join();
    ASSERT_TRUE(consumer_returned.load(std::memory_order_acquire));
    ASSERT_FALSE(take_ok);
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_take_with_invalid_worker_id_fallback) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    queue.force_put(make_wg_task(_wg1, 5));
    auto result = queue.take(kNumWorkers);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value().workgroup, _wg1);
    ASSERT_EQ(queue.size(), 0);
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_try_offer) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    ASSERT_TRUE(queue.try_offer(make_wg_task(_wg1, 10)));
    ASSERT_EQ(queue.size(), 1);

    auto result = queue.take(1);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(queue.size(), 0);
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_workgroup_vruntime_selection) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    auto* entity2 = const_cast<WorkGroupScanSchedEntity*>(_wg2->scan_sched_entity());
    entity2->incr_runtime_ns(1'000'000'000L);

    queue.force_put(make_wg_task(_wg2, 5));
    queue.force_put(make_wg_task(_wg1, 5));

    auto first = queue.take(0);
    ASSERT_TRUE(first.ok());
    ASSERT_EQ(first.value().workgroup, _wg1);

    auto second = queue.take(1);
    ASSERT_TRUE(second.ok());
    ASSERT_EQ(second.value().workgroup, _wg2);

    ASSERT_EQ(queue.size(), 0);
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_update_statistics) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    ScanTask task = make_wg_task(_wg1, 5);

    int64_t before = _wg1->scan_sched_entity()->vruntime_ns();
    queue.update_statistics(task, 500'000'000L);
    int64_t after = _wg1->scan_sched_entity()->vruntime_ns();

    ASSERT_GT(after, before);
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_should_yield_refreshes_min_wg_on_enqueue) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    auto* entity2 = const_cast<WorkGroupScanSchedEntity*>(_wg2->scan_sched_entity());
    entity2->incr_runtime_ns(1'000'000'000L);

    ASSERT_FALSE(queue.should_yield(_wg2.get(), 0));

    queue.force_put(make_wg_task(_wg1, 5));

    ASSERT_TRUE(queue.should_yield(_wg2.get(), 0));
}

} // namespace starrocks::workgroup
