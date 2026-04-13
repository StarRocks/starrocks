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

// Test that tasks are dequeued in strict priority order (highest first).
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_priority_ordering) {
    constexpr int NUM_WORKERS = 4;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

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

// Test that try_offer enqueues a task and it can be dequeued.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_try_offer) {
    constexpr int NUM_WORKERS = 2;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

    ASSERT_TRUE(queue.try_offer(make_task(7), 0));

    ScanTask out;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 7);
}

// Test that the external producer variant (no worker_id) works correctly.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_external_producer) {
    constexpr int NUM_WORKERS = 2;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

    queue.force_put(make_task(12));

    ASSERT_EQ(queue.size(), 1);

    ScanTask out;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 12);
    ASSERT_EQ(queue.size(), 0);
}

// Test that try_take on an empty queue returns false and size() is 0.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_empty) {
    constexpr int NUM_WORKERS = 2;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

    ScanTask out;
    ASSERT_FALSE(queue.try_take(out));
    ASSERT_EQ(queue.size(), 0);
}

// Test concurrent enqueue/dequeue with multiple worker threads.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_multithread) {
    constexpr int NUM_WORKERS = 4;
    constexpr int ITEMS_PER_WORKER = 1000;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

    // Producer threads: each worker enqueues ITEMS_PER_WORKER tasks.
    std::vector<std::thread> producers;
    producers.reserve(NUM_WORKERS);
    for (int w = 0; w < NUM_WORKERS; ++w) {
        producers.emplace_back([&queue, w]() {
            for (int i = 0; i < ITEMS_PER_WORKER; ++i) {
                int priority = i % LockFreeScanTaskQueue::NUM_PRIORITY_LEVELS;
                queue.force_put(make_task(priority), w);
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    ASSERT_EQ(queue.size(), NUM_WORKERS * ITEMS_PER_WORKER);

    // Consumer threads: dequeue all items.
    std::atomic<int> total_dequeued{0};
    std::vector<std::thread> consumers;
    consumers.reserve(NUM_WORKERS);
    for (int w = 0; w < NUM_WORKERS; ++w) {
        consumers.emplace_back([&queue, &total_dequeued]() {
            ScanTask out;
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

// Test boundary priorities: 0 (lowest) and 20 (highest).
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_boundary_priorities) {
    constexpr int NUM_WORKERS = 2;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

    queue.force_put(make_task(0), 0);
    queue.force_put(make_task(20), 0);

    ScanTask out;

    // Highest priority (20) should come first.
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 20);

    // Lowest priority (0) next.
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(out.priority, 0);

    ASSERT_FALSE(queue.try_take(out));
}

// Test multiple tasks at the same priority level — all should be dequeued (no loss).
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_same_priority) {
    constexpr int NUM_WORKERS = 2;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

    constexpr int COUNT = 100;
    for (int i = 0; i < COUNT; ++i) {
        queue.force_put(make_task(10), 0);
    }
    ASSERT_EQ(queue.size(), COUNT);

    int dequeued = 0;
    ScanTask out;
    while (queue.try_take(out)) {
        ASSERT_EQ(out.priority, 10);
        ++dequeued;
    }
    ASSERT_EQ(dequeued, COUNT);
}

} // namespace starrocks::workgroup
