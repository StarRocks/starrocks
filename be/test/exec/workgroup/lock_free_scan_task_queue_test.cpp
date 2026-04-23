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

#include "exec/workgroup/lock_free_scan_task_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "base/testutil/parallel_test.h"

namespace starrocks::workgroup {

static ScanTask make_task(int priority) {
    ScanTask task([](YieldContext&) {});
    task.priority = priority;
    return task;
}

PARALLEL_TEST(LockFreeScanTaskQueueTest, test_priority_ordering) {
    constexpr int kNumWorkers = 4;
    LockFreeScanTaskQueue queue(kNumWorkers);

    queue.force_put(make_task(5), 0);
    queue.force_put(make_task(20), 0);
    queue.force_put(make_task(10), 0);

    ScanTask out;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 20);

    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 10);

    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 5);

    ASSERT_FALSE(queue.try_take(out));
}

PARALLEL_TEST(LockFreeScanTaskQueueTest, test_try_offer) {
    constexpr int kNumWorkers = 2;
    LockFreeScanTaskQueue queue(kNumWorkers);

    ASSERT_TRUE(queue.try_offer(make_task(7), 0));

    ScanTask out;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 7);
}

PARALLEL_TEST(LockFreeScanTaskQueueTest, test_external_producer) {
    constexpr int kNumWorkers = 2;
    LockFreeScanTaskQueue queue(kNumWorkers);

    queue.force_put(make_task(12));

    ASSERT_EQ(queue.size(), 1);

    ScanTask out;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 12);
    ASSERT_EQ(queue.size(), 0);
}

PARALLEL_TEST(LockFreeScanTaskQueueTest, test_empty) {
    constexpr int kNumWorkers = 2;
    LockFreeScanTaskQueue queue(kNumWorkers);

    ScanTask out;
    ASSERT_FALSE(queue.try_take(out));
    ASSERT_EQ(queue.size(), 0);
}

PARALLEL_TEST(LockFreeScanTaskQueueTest, test_multithread) {
    constexpr int kNumWorkers = 4;
    constexpr int kItemsPerWorker = 1000;
    LockFreeScanTaskQueue queue(kNumWorkers);

    std::vector<std::thread> producers;
    producers.reserve(kNumWorkers);
    for (int worker = 0; worker < kNumWorkers; ++worker) {
        producers.emplace_back([&queue, worker]() {
            for (int i = 0; i < kItemsPerWorker; ++i) {
                int priority = i % LockFreeScanTaskQueue::NUM_PRIORITY_LEVELS;
                queue.force_put(make_task(priority), worker);
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }

    ASSERT_EQ(queue.size(), kNumWorkers * kItemsPerWorker);

    std::atomic<int> total_dequeued{0};
    std::vector<std::thread> consumers;
    consumers.reserve(kNumWorkers);
    for (int worker = 0; worker < kNumWorkers; ++worker) {
        consumers.emplace_back([&queue, &total_dequeued]() {
            ScanTask out;
            while (queue.try_take(out)) {
                total_dequeued.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& consumer : consumers) {
        consumer.join();
    }

    ASSERT_EQ(total_dequeued.load(), kNumWorkers * kItemsPerWorker);
    ASSERT_EQ(queue.size(), 0);
}

PARALLEL_TEST(LockFreeScanTaskQueueTest, test_boundary_priorities) {
    constexpr int kNumWorkers = 2;
    LockFreeScanTaskQueue queue(kNumWorkers);

    queue.force_put(make_task(0), 0);
    queue.force_put(make_task(20), 0);

    ScanTask out;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 20);

    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 0);

    ASSERT_FALSE(queue.try_take(out));
}

PARALLEL_TEST(LockFreeScanTaskQueueTest, test_same_priority) {
    constexpr int kNumWorkers = 2;
    constexpr int kCount = 100;
    LockFreeScanTaskQueue queue(kNumWorkers);

    for (int i = 0; i < kCount; ++i) {
        queue.force_put(make_task(10), 0);
    }
    ASSERT_EQ(queue.size(), kCount);

    int dequeued = 0;
    ScanTask out;
    while (queue.try_take(out)) {
        ASSERT_EQ(out.priority, 10);
        ++dequeued;
    }
    ASSERT_EQ(dequeued, kCount);
}

} // namespace starrocks::workgroup
