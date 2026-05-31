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

#include "exec/pipeline/lock_free_driver_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "base/testutil/parallel_test.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/query_context.h"

namespace starrocks::pipeline {

class MockEmptyOperator2 final : public SourceOperator {
public:
    MockEmptyOperator2(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "mock_empty_operator2", plan_node_id, false, driver_sequence) {}
    ~MockEmptyOperator2() override = default;

    bool has_output() const override { return true; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return true; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

static Operators gen_operators() {
    Operators ops;
    ops.emplace_back(std::make_shared<MockEmptyOperator2>(nullptr, 1, 1, 0));
    return ops;
}

// Test that drivers with lower accumulated time (lower level) are dequeued
// before drivers with higher accumulated time (higher level).
PARALLEL_TEST(LockFreeDriverQueueTest, test_level_computation) {
    constexpr int NUM_WORKERS = 4;
    LockFreeDriverQueue queue(NUM_WORKERS);

    QueryContext query_context;

    // Driver with low accumulated time -> should land at level 0.
    auto driver_low = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    driver_low->set_driver_queue_level(0);
    // No accumulated time, so it stays at level 0.

    // Driver with very high accumulated time -> should land at level 7 (max).
    auto driver_high = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    driver_high->set_driver_queue_level(0);
    // Accumulate enough time to push it to the highest level.
    // With default base=200ms, level 7 threshold = sum(200ms*(i+1) for i=0..7) = 7.2s = 7,200,000,000 ns
    // We set accumulated time above that threshold.
    driver_high->driver_acct().update_last_time_spent(8'000'000'000L);

    queue.put_back(driver_high.get(), 0);
    queue.put_back(driver_low.get(), 0);

    // The low-level driver should come out first because level 0 has lower
    // weighted accu_time than level 7.
    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out, driver_low.get());

    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out, driver_high.get());
}

// Test that update_statistics influences level selection. When level 0's
// accumulated time is inflated, a driver at level 1 should be dequeued first.
PARALLEL_TEST(LockFreeDriverQueueTest, test_weighted_level_selection) {
    constexpr int NUM_WORKERS = 4;
    LockFreeDriverQueue queue(NUM_WORKERS);

    QueryContext query_context;

    // Inflate level 0's accumulated time so it has high weighted time.
    queue.update_statistics(0, 10'000'000'000L);

    // Create a driver that will land at level 0 (low accumulated time).
    auto driver0 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    driver0->set_driver_queue_level(0);

    // Create a driver that will land at level 1.
    // Accumulated time must be >= _level_time_slices[0] (200ms) but < _level_time_slices[1] (600ms).
    auto driver1 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    driver1->set_driver_queue_level(0);
    driver1->driver_acct().update_last_time_spent(300'000'000L);

    queue.put_back(driver0.get(), 0);
    queue.put_back(driver1.get(), 1);

    // Level 1 should be selected first because level 0 has inflated accu_time.
    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out, driver1.get());

    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out, driver0.get());
}

// Test that try_take on an empty queue returns false.
PARALLEL_TEST(LockFreeDriverQueueTest, test_empty) {
    constexpr int NUM_WORKERS = 2;
    LockFreeDriverQueue queue(NUM_WORKERS);

    DriverRawPtr out = nullptr;
    ASSERT_FALSE(queue.try_take(out));
    ASSERT_EQ(queue.size(), 0);
}

// Test that put_back without worker_id (external producer) works correctly.
PARALLEL_TEST(LockFreeDriverQueueTest, test_external_producer) {
    constexpr int NUM_WORKERS = 2;
    LockFreeDriverQueue queue(NUM_WORKERS);

    QueryContext query_context;

    auto driver = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    driver->set_driver_queue_level(0);

    // Use the external producer variant (no worker_id).
    queue.put_back(driver.get());

    ASSERT_EQ(queue.size(), 1);

    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out, driver.get());
    ASSERT_EQ(queue.size(), 0);
}

// Test concurrent enqueue/dequeue with multiple worker threads.
PARALLEL_TEST(LockFreeDriverQueueTest, test_multithread) {
    constexpr int NUM_WORKERS = 4;
    constexpr int ITEMS_PER_WORKER = 1000;
    LockFreeDriverQueue queue(NUM_WORKERS);

    QueryContext query_context;

    // Pre-create all drivers.
    std::vector<std::vector<DriverPtr>> all_drivers(NUM_WORKERS);
    for (int w = 0; w < NUM_WORKERS; ++w) {
        all_drivers[w].reserve(ITEMS_PER_WORKER);
        for (int i = 0; i < ITEMS_PER_WORKER; ++i) {
            auto d = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
            d->set_driver_queue_level(0);
            all_drivers[w].push_back(std::move(d));
        }
    }

    // Producer threads: each worker enqueues its drivers.
    std::vector<std::thread> producers;
    producers.reserve(NUM_WORKERS);
    for (int w = 0; w < NUM_WORKERS; ++w) {
        producers.emplace_back([&queue, &all_drivers, w]() {
            for (int i = 0; i < ITEMS_PER_WORKER; ++i) {
                queue.put_back(all_drivers[w][i].get(), w);
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    // All items should now be in the queue.
    ASSERT_EQ(queue.size(), NUM_WORKERS * ITEMS_PER_WORKER);

    // Consumer threads: dequeue all items.
    std::atomic<int> total_dequeued{0};
    std::vector<std::thread> consumers;
    consumers.reserve(NUM_WORKERS);
    for (int w = 0; w < NUM_WORKERS; ++w) {
        consumers.emplace_back([&queue, &total_dequeued]() {
            DriverRawPtr out = nullptr;
            while (queue.try_take(out)) {
                total_dequeued.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : consumers) {
        t.join();
    }

    ASSERT_EQ(total_dequeued.load(), NUM_WORKERS * ITEMS_PER_WORKER);
    ASSERT_EQ(queue.size(), 0);
}

// Test that after draining one level, items at other levels are still dequeued.
PARALLEL_TEST(LockFreeDriverQueueTest, test_bitmap_after_partial_drain) {
    constexpr int NUM_WORKERS = 2;
    LockFreeDriverQueue queue(NUM_WORKERS);

    QueryContext query_context;

    // Driver at level 0 (low accumulated time).
    auto d0 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    d0->set_driver_queue_level(0);

    // Driver at level 7 (high accumulated time).
    auto d7 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    d7->set_driver_queue_level(0);
    d7->driver_acct().update_last_time_spent(8'000'000'000L);

    queue.put_back(d0.get(), 0);
    queue.put_back(d7.get(), 0);
    ASSERT_EQ(queue.size(), 2);

    // Take level 0 driver first (lower weighted time).
    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out, d0.get());

    // Level 0 is now empty, but level 7 still has an item.
    // Bitmap should reflect this and try_take should succeed.
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out, d7.get());

    // Now fully empty.
    ASSERT_FALSE(queue.try_take(out));
    ASSERT_EQ(queue.size(), 0);
}

} // namespace starrocks::pipeline
