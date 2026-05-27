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

#include "base/concurrency/countdown_latch.h"
#include "common/thread/thread.h"
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

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_successful_dequeue_drains_wakeup_permit) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    queue.force_put(make_wg_task(_wg1, 5));
    ASSERT_EQ(queue.available_wakeup_permits_for_test(), 1u);

    auto result = queue.take(0);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(queue.size(), 0);
    ASSERT_EQ(queue.available_wakeup_permits_for_test(), 0u);
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

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_take_skips_yield_blocked_workgroups) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    auto* manager = ExecEnv::GetInstance()->workgroup_manager();
    auto& executors_manager = manager->_executors_manager;
    const bool old_enable_cpu_borrowing = executors_manager._conf.enable_cpu_borrowing;
    auto& cpu_owner = executors_manager._cpu_owners[0];
    WorkGroup* old_owner = cpu_owner.raw_wg.load(std::memory_order_relaxed);

    executors_manager._conf.enable_cpu_borrowing = true;
    cpu_owner.set_wg(_wg1.get());
    auto token = _wg1->acquire_running_query_token(false).value();

    queue.force_put(make_wg_task(_wg2, 5));

    CountDownLatch started(1);
    std::atomic<bool> got_task{false};
    std::atomic<bool> got_cancelled{false};
    scoped_refptr<Thread> worker;
    ASSERT_TRUE(Thread::create(
                        "test", "yield_blocked_scan_take",
                        [&]() {
                            Thread::current_thread()->set_first_bound_cpuid(0);
                            started.count_down();
                            auto result = queue.take(0);
                            got_task.store(result.ok(), std::memory_order_release);
                            got_cancelled.store(result.status().is_cancelled(), std::memory_order_release);
                        },
                        &worker)
                        .ok());

    started.wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ASSERT_FALSE(got_task.load(std::memory_order_acquire));

    queue.close();
    worker->join();

    ASSERT_TRUE(got_cancelled.load(std::memory_order_acquire));

    cpu_owner.set_wg(old_owner);
    executors_manager._conf.enable_cpu_borrowing = old_enable_cpu_borrowing;
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_rebases_returning_workgroup_vruntime_on_enqueue) {
    constexpr int kNumWorkers = 4;
    LockFreeWorkGroupScanTaskQueue queue(ScanSchedEntityType::OLAP, kNumWorkers);

    auto wg3 = std::make_shared<WorkGroup>("scan_wg3", 30, WorkGroup::DEFAULT_VERSION, 1, 0.5, 10, 1.0,
                                           WorkGroupType::WG_NORMAL, WorkGroup::DEFAULT_MEM_POOL);
    auto wg4 = std::make_shared<WorkGroup>("scan_wg4", 40, WorkGroup::DEFAULT_VERSION, 2, 0.5, 10, 1.0,
                                           WorkGroupType::WG_NORMAL, WorkGroup::DEFAULT_MEM_POOL);
    wg3 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(wg3);
    wg4 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(wg4);

    auto* entity1 = const_cast<WorkGroupScanSchedEntity*>(wg3->scan_sched_entity());
    auto* entity2 = const_cast<WorkGroupScanSchedEntity*>(wg4->scan_sched_entity());
    entity2->incr_runtime_ns(1'000'000'000L);

    int64_t before = entity1->vruntime_ns();
    queue.force_put(make_wg_task(wg4, 5));

    queue.force_put(make_wg_task(wg3, 5));

    ASSERT_GT(entity1->vruntime_ns(), before);
    ASSERT_LT(entity1->vruntime_ns(), entity2->vruntime_ns());
}

} // namespace starrocks::workgroup
