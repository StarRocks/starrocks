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

#include "exec/pipeline/multi_level_concurrent_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "base/testutil/parallel_test.h"

namespace starrocks::pipeline {

// test_single_level_basic:
// Enqueue 2 items on level 0 with different workers, dequeue both, verify empty.
PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_single_level_basic) {
    MultiLevelConcurrentQueue<int, 1> queue(2);

    queue.enqueue(10, 0, 0);
    queue.enqueue(20, 0, 1);

    ASSERT_FALSE(queue.empty(0));
    ASSERT_EQ(queue.size_approx(0), 2);

    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    // moodycamel does not guarantee FIFO across producers, so we just check
    // both values are retrieved.
    int first = item;

    ASSERT_TRUE(queue.try_dequeue(0, item));
    int second = item;

    // Verify both items were dequeued (order may vary).
    ASSERT_EQ(first + second, 30);
    ASSERT_TRUE(queue.empty(0));
    ASSERT_FALSE(queue.try_dequeue(0, item));
}

// test_level_isolation:
// Enqueue to levels 0, 1, 2. Verify level 3 is empty. Dequeue from each level
// gets the correct items.
PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_level_isolation) {
    MultiLevelConcurrentQueue<int, 4> queue(1);

    queue.enqueue(100, 0, 0);
    queue.enqueue(200, 1, 0);
    queue.enqueue(300, 2, 0);

    // Level 3 should be empty.
    ASSERT_TRUE(queue.empty(3));
    int item = 0;
    ASSERT_FALSE(queue.try_dequeue(3, item));

    // Dequeue from each level and verify the correct item.
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(item, 100);

    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_EQ(item, 200);

    ASSERT_TRUE(queue.try_dequeue(2, item));
    ASSERT_EQ(item, 300);

    // All levels should now be empty.
    for (int l = 0; l < 4; ++l) {
        ASSERT_TRUE(queue.empty(l));
    }
}

// test_implicit_producer:
// Enqueue without worker_id (implicit producer path), dequeue successfully.
PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_implicit_producer) {
    MultiLevelConcurrentQueue<int, 2> queue(1);

    // Use the implicit producer overload (no worker_id).
    queue.enqueue(42, 0);
    queue.enqueue(99, 1);

    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(item, 42);

    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_EQ(item, 99);
}

// test_size_and_empty:
// Verify empty(), size_approx() after enqueue/dequeue.
PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_size_and_empty) {
    MultiLevelConcurrentQueue<int, 2> queue(2);

    // Initially empty.
    ASSERT_TRUE(queue.empty(0));
    ASSERT_TRUE(queue.empty(1));
    ASSERT_EQ(queue.size_approx(0), 0);
    ASSERT_EQ(queue.size_approx(1), 0);
    ASSERT_EQ(queue.size_approx(), 0);

    // Enqueue items.
    queue.enqueue(1, 0, 0);
    queue.enqueue(2, 0, 1);
    queue.enqueue(3, 1, 0);

    ASSERT_FALSE(queue.empty(0));
    ASSERT_FALSE(queue.empty(1));
    ASSERT_EQ(queue.size_approx(0), 2);
    ASSERT_EQ(queue.size_approx(1), 1);
    ASSERT_EQ(queue.size_approx(), 3);

    // Dequeue one item from level 0.
    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(queue.size_approx(0), 1);
    ASSERT_EQ(queue.size_approx(), 2);

    // Dequeue remaining items.
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_TRUE(queue.empty(0));
    ASSERT_TRUE(queue.empty(1));
    ASSERT_EQ(queue.size_approx(), 0);
}

// test_multithread_correctness:
// 8 workers x 10000 items, verify all items are accounted for (count + sum check).
PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_multithread_correctness) {
    constexpr int kNumWorkers = 8;
    constexpr int kItemsPerWorker = 10000;
    constexpr int kNumLevels = 4;

    MultiLevelConcurrentQueue<int, kNumLevels> queue(kNumWorkers);

    // Expected sum: each worker enqueues values [0, kItemsPerWorker).
    // Total sum = kNumWorkers * kItemsPerWorker * (kItemsPerWorker - 1) / 2
    const int64_t expected_sum = static_cast<int64_t>(kNumWorkers) * kItemsPerWorker * (kItemsPerWorker - 1) / 2;
    const int64_t expected_count = static_cast<int64_t>(kNumWorkers) * kItemsPerWorker;

    // Producer threads.
    std::vector<std::thread> producers;
    producers.reserve(kNumWorkers);
    for (int w = 0; w < kNumWorkers; ++w) {
        producers.emplace_back([&queue, w]() {
            for (int i = 0; i < kItemsPerWorker; ++i) {
                int level = i % kNumLevels;
                queue.enqueue(i, level, w);
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    // Consumer threads: drain the queue from all levels.
    std::atomic<int64_t> total_sum{0};
    std::atomic<int64_t> total_count{0};

    std::vector<std::thread> consumers;
    consumers.reserve(kNumWorkers);
    for (int w = 0; w < kNumWorkers; ++w) {
        consumers.emplace_back([&queue, &total_sum, &total_count]() {
            int64_t local_sum = 0;
            int64_t local_count = 0;
            int item = 0;
            // Each consumer tries to dequeue from all levels in round-robin.
            bool found = true;
            while (found) {
                found = false;
                for (int l = 0; l < kNumLevels; ++l) {
                    while (queue.try_dequeue(l, item)) {
                        local_sum += item;
                        local_count++;
                        found = true;
                    }
                }
            }
            total_sum.fetch_add(local_sum);
            total_count.fetch_add(local_count);
        });
    }

    for (auto& t : consumers) {
        t.join();
    }

    ASSERT_EQ(total_count.load(), expected_count);
    ASSERT_EQ(total_sum.load(), expected_sum);
}

// test_move_only_type:
// Enqueue/dequeue std::unique_ptr<int>.
PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_move_only_type) {
    MultiLevelConcurrentQueue<std::unique_ptr<int>, 2> queue(1);

    queue.enqueue(std::make_unique<int>(42), 0, 0);
    queue.enqueue(std::make_unique<int>(99), 1);

    std::unique_ptr<int> item;

    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_NE(item, nullptr);
    ASSERT_EQ(*item, 42);

    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_NE(item, nullptr);
    ASSERT_EQ(*item, 99);

    // Both levels should be empty now.
    ASSERT_TRUE(queue.empty(0));
    ASSERT_TRUE(queue.empty(1));
}

} // namespace starrocks::pipeline
