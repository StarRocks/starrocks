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

PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_single_level_basic) {
    MultiLevelConcurrentQueue<int, 1> queue(2);

    ASSERT_TRUE(queue.enqueue(10, 0, 0));
    ASSERT_TRUE(queue.enqueue(20, 0, 1));

    ASSERT_FALSE(queue.empty_approx(0));
    ASSERT_EQ(queue.size_approx(0), 2);

    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    int first = item;

    ASSERT_TRUE(queue.try_dequeue(0, item));
    int second = item;

    ASSERT_EQ(first + second, 30);
    ASSERT_TRUE(queue.empty_approx(0));
    ASSERT_FALSE(queue.try_dequeue(0, item));
}

PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_level_isolation) {
    MultiLevelConcurrentQueue<int, 4> queue(1);

    ASSERT_TRUE(queue.enqueue(100, 0, 0));
    ASSERT_TRUE(queue.enqueue(200, 1, 0));
    ASSERT_TRUE(queue.enqueue(300, 2, 0));

    ASSERT_TRUE(queue.empty_approx(3));
    int item = 0;
    ASSERT_FALSE(queue.try_dequeue(3, item));

    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(item, 100);

    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_EQ(item, 200);

    ASSERT_TRUE(queue.try_dequeue(2, item));
    ASSERT_EQ(item, 300);

    for (int l = 0; l < 4; ++l) {
        ASSERT_TRUE(queue.empty_approx(l));
    }
}

PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_implicit_producer) {
    MultiLevelConcurrentQueue<int, 2> queue(1);

    ASSERT_TRUE(queue.enqueue(42, 0));
    ASSERT_TRUE(queue.enqueue(99, 1));

    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(item, 42);

    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_EQ(item, 99);
}

PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_size_and_empty_approx) {
    MultiLevelConcurrentQueue<int, 2> queue(2);

    ASSERT_TRUE(queue.empty_approx(0));
    ASSERT_TRUE(queue.empty_approx(1));
    ASSERT_EQ(queue.size_approx(0), 0);
    ASSERT_EQ(queue.size_approx(1), 0);
    ASSERT_EQ(queue.size_approx(), 0);

    ASSERT_TRUE(queue.enqueue(1, 0, 0));
    ASSERT_TRUE(queue.enqueue(2, 0, 1));
    ASSERT_TRUE(queue.enqueue(3, 1, 0));

    ASSERT_FALSE(queue.empty_approx(0));
    ASSERT_FALSE(queue.empty_approx(1));
    ASSERT_EQ(queue.size_approx(0), 2);
    ASSERT_EQ(queue.size_approx(1), 1);
    ASSERT_EQ(queue.size_approx(), 3);

    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(queue.size_approx(0), 1);
    ASSERT_EQ(queue.size_approx(), 2);

    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_TRUE(queue.empty_approx(0));
    ASSERT_TRUE(queue.empty_approx(1));
    ASSERT_EQ(queue.size_approx(), 0);
}

PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_multithread_correctness) {
    constexpr int kNumWorkers = 8;
    constexpr int kItemsPerWorker = 10000;
    constexpr int kNumLevels = 4;

    MultiLevelConcurrentQueue<int, kNumLevels> queue(kNumWorkers);

    const int64_t expected_sum = static_cast<int64_t>(kNumWorkers) * kItemsPerWorker * (kItemsPerWorker - 1) / 2;
    const int64_t expected_count = static_cast<int64_t>(kNumWorkers) * kItemsPerWorker;

    std::vector<std::thread> producers;
    producers.reserve(kNumWorkers);
    for (int w = 0; w < kNumWorkers; ++w) {
        producers.emplace_back([&queue, w]() {
            for (int i = 0; i < kItemsPerWorker; ++i) {
                ASSERT_TRUE(queue.enqueue(i, i % kNumLevels, w));
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    std::atomic<int64_t> total_sum{0};
    std::atomic<int64_t> total_count{0};

    std::vector<std::thread> consumers;
    consumers.reserve(kNumWorkers);
    for (int w = 0; w < kNumWorkers; ++w) {
        consumers.emplace_back([&queue, &total_sum, &total_count]() {
            int64_t local_sum = 0;
            int64_t local_count = 0;
            int item = 0;
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

PARALLEL_TEST(MultiLevelConcurrentQueueTest, test_move_only_type) {
    MultiLevelConcurrentQueue<std::unique_ptr<int>, 2> queue(1);

    ASSERT_TRUE(queue.enqueue(std::make_unique<int>(42), 0, 0));
    ASSERT_TRUE(queue.enqueue(std::make_unique<int>(99), 1));

    std::unique_ptr<int> item;

    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_NE(item, nullptr);
    ASSERT_EQ(*item, 42);

    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_NE(item, nullptr);
    ASSERT_EQ(*item, 99);

    ASSERT_TRUE(queue.empty_approx(0));
    ASSERT_TRUE(queue.empty_approx(1));
}

} // namespace starrocks::pipeline
