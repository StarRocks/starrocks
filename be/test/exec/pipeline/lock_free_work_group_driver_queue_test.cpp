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

#include "exec/pipeline/lock_free_work_group_driver_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

class MockWgDriverQueueOp final : public SourceOperator {
public:
    MockWgDriverQueueOp() : SourceOperator(nullptr, 1, "mock_wg_driver_queue_op", 1, false, 0) {}
    ~MockWgDriverQueueOp() override = default;

    bool has_output() const override { return true; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return true; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

static Operators gen_wg_ops() {
    Operators ops;
    ops.emplace_back(std::make_shared<MockWgDriverQueueOp>());
    return ops;
}

class LockFreeWorkGroupDriverQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        _wg1 = std::make_shared<workgroup::WorkGroup>("wg_lf1", 1001, workgroup::WorkGroup::DEFAULT_VERSION, 1, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                      workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg2 = std::make_shared<workgroup::WorkGroup>("wg_lf2", 1002, workgroup::WorkGroup::DEFAULT_VERSION, 2, 0.5, 10,
                                                      1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                      workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg1 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg1);
        _wg2 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg2);
    }

protected:
    PipelineExecutorMetrics _metrics;
    workgroup::WorkGroupPtr _wg1;
    workgroup::WorkGroupPtr _wg2;
};

// Test: put_back one driver into a single workgroup, take it back non-blocking.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_single_workgroup) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    QueryContext query_ctx;
    auto driver = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver->set_driver_queue_level(0);
    driver->set_workgroup(_wg1);

    queue.put_back(driver.get());
    ASSERT_EQ(queue.size(), 1);

    auto result = queue.take(false);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value(), driver.get());
    ASSERT_EQ(queue.size(), 0);
}

// Test: put_back two drivers, cancel one, take should return both but the
// cancelled one has CANCELED state.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_cancel) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    QueryContext query_ctx;
    auto driver1 = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver1->set_driver_queue_level(0);
    driver1->set_workgroup(_wg1);

    auto driver2 = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver2->set_driver_queue_level(0);
    driver2->set_workgroup(_wg1);

    queue.put_back(driver1.get());
    queue.put_back(driver2.get());
    ASSERT_EQ(queue.size(), 2);

    // Cancel driver2 while it is still in the queue.
    queue.cancel(driver2.get());

    // Take both drivers. One of them should have CANCELED state.
    auto r1 = queue.take(false);
    auto r2 = queue.take(false);
    ASSERT_TRUE(r1.ok() && r1.value() != nullptr);
    ASSERT_TRUE(r2.ok() && r2.value() != nullptr);
    ASSERT_EQ(queue.size(), 0);

    auto* out1 = r1.value();
    auto* out2 = r2.value();

    // Find which one was cancelled.
    if (out1 == driver2.get()) {
        ASSERT_EQ(out1->driver_state(), DriverState::CANCELED);
    } else if (out2 == driver2.get()) {
        ASSERT_EQ(out2->driver_state(), DriverState::CANCELED);
    } else {
        FAIL() << "Neither dequeued driver is driver2";
    }
}

// Test: consumer thread blocks on take, producer puts after a short delay,
// consumer should unblock and receive the driver.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_blocking_wakeup) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    QueryContext query_ctx;
    auto driver = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver->set_driver_queue_level(0);
    driver->set_workgroup(_wg1);

    std::atomic<bool> consumer_got_driver{false};
    DriverRawPtr consumer_result = nullptr;

    auto consumer_thread = std::thread([&]() {
        auto result = queue.take(true);
        if (result.ok() && result.value() != nullptr) {
            consumer_result = result.value();
            consumer_got_driver.store(true, std::memory_order_release);
        }
    });

    // Wait a bit to ensure the consumer is blocked.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(consumer_got_driver.load(std::memory_order_acquire));

    // Put a driver; the consumer should unblock.
    queue.put_back(driver.get());

    consumer_thread.join();
    ASSERT_TRUE(consumer_got_driver.load(std::memory_order_acquire));
    ASSERT_EQ(consumer_result, driver.get());
}

// Test: consumer blocks on take, close() wakes it and take returns Cancelled.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_close) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    std::atomic<bool> consumer_returned{false};
    bool take_ok = true;

    auto consumer_thread = std::thread([&]() {
        auto result = queue.take(true);
        take_ok = result.ok() && result.value() != nullptr;
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

// Test: two workgroups with different vruntimes. The workgroup with lower
// vruntime should be selected first.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_workgroup_vruntime_selection) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    QueryContext query_ctx;

    auto driver1 = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver1->set_driver_queue_level(0);
    driver1->set_workgroup(_wg1);

    auto driver2 = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver2->set_driver_queue_level(0);
    driver2->set_workgroup(_wg2);

    // Inflate wg2's vruntime so wg1 has lower vruntime and should be picked first.
    auto* entity2 = _wg2->driver_sched_entity();
    entity2->incr_runtime_ns(1'000'000'000L);

    queue.put_back(driver2.get());
    queue.put_back(driver1.get());

    // wg1 has lower vruntime, so driver1 should come out first.
    auto r1 = queue.take(false);
    ASSERT_TRUE(r1.ok());
    ASSERT_EQ(r1.value(), driver1.get());

    auto r2 = queue.take(false);
    ASSERT_TRUE(r2.ok());
    ASSERT_EQ(r2.value(), driver2.get());

    ASSERT_EQ(queue.size(), 0);
}

// Test: update_statistics increments workgroup vruntime.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_update_statistics) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    QueryContext query_ctx;
    auto driver = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver->set_driver_queue_level(0);
    driver->set_workgroup(_wg1);
    driver->driver_acct().update_last_time_spent(500'000'000L);

    int64_t vruntime_before = _wg1->driver_sched_entity()->vruntime_ns();
    queue.update_statistics(driver.get());
    int64_t vruntime_after = _wg1->driver_sched_entity()->vruntime_ns();

    ASSERT_GT(vruntime_after, vruntime_before);
}

// Test: non-blocking take on empty queue returns nullptr.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_empty_take) {
    constexpr int NUM_WORKERS = 2;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    auto result = queue.take(false);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value(), nullptr);
    ASSERT_EQ(queue.size(), 0);
}

// Test: driver state is managed correctly by put_back/take.
TEST_F(LockFreeWorkGroupDriverQueueTest, test_driver_state_management) {
    constexpr int NUM_WORKERS = 4;
    LockFreeWorkGroupDriverQueue queue(_metrics.get_driver_queue_metrics(), NUM_WORKERS);

    QueryContext query_ctx;
    auto driver = std::make_shared<PipelineDriver>(gen_wg_ops(), &query_ctx, nullptr, nullptr, -1);
    driver->set_driver_queue_level(0);
    driver->set_workgroup(_wg1);

    ASSERT_FALSE(driver->is_in_ready());

    queue.put_back(driver.get());
    ASSERT_TRUE(driver->is_in_ready());

    auto result = queue.take(false);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_in_ready());
}

} // namespace starrocks::pipeline
