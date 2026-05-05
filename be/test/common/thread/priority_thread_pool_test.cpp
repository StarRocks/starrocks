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

#include "common/thread/priority_thread_pool.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <vector>

#include "common/thread/blocking_priority_queue.hpp"

namespace starrocks {

namespace {

struct QueueTask {
    int priority = 0;
    int id = 0;

    bool operator<(const QueueTask& rhs) const { return priority < rhs.priority; }

    QueueTask& operator++() {
        priority += 2;
        return *this;
    }
};

} // namespace

TEST(BlockingPriorityQueueTest, GetHighestPriorityFirstAndShutdown) {
    BlockingPriorityQueue<QueueTask> queue(2);

    EXPECT_TRUE(queue.try_put(QueueTask{1, 1}));
    EXPECT_TRUE(queue.try_put(QueueTask{5, 2}));
    EXPECT_FALSE(queue.try_put(QueueTask{10, 3}));
    EXPECT_EQ(2, queue.get_size());
    EXPECT_EQ(2, queue.get_capacity());

    QueueTask task;
    ASSERT_TRUE(queue.non_blocking_get(&task));
    EXPECT_EQ(2, task.id);

    ASSERT_TRUE(queue.blocking_get(&task));
    EXPECT_EQ(1, task.id);

    EXPECT_FALSE(queue.non_blocking_get(&task));
    queue.shutdown();
    EXPECT_FALSE(queue.blocking_put(QueueTask{1, 4}));
    EXPECT_FALSE(queue.blocking_get(&task));
}

TEST(PriorityThreadPoolTest, OfferTasksAndDrainShutdown) {
    PriorityThreadPool pool("prio_ut", 1, 4);
    std::mutex lock;
    std::vector<int> values;

    EXPECT_EQ(4, pool.get_queue_capacity());
    EXPECT_TRUE(pool.offer([&] {
        std::lock_guard<std::mutex> guard(lock);
        values.push_back(1);
    }));
    EXPECT_TRUE(pool.try_offer([&] {
        std::lock_guard<std::mutex> guard(lock);
        values.push_back(2);
    }));

    pool.drain_and_shutdown();

    std::sort(values.begin(), values.end());
    EXPECT_EQ((std::vector<int>{1, 2}), values);
    EXPECT_EQ(0, pool.get_queue_size());
}

TEST(PriorityThreadPoolTest, TryOfferFailsWhenQueueIsFull) {
    PriorityThreadPool pool("prio_full", 0, 1);

    EXPECT_TRUE(pool.try_offer([] {}));
    EXPECT_FALSE(pool.try_offer([] {}));
    EXPECT_EQ(1, pool.get_queue_size());

    pool.shutdown();
    EXPECT_FALSE(pool.try_offer([] {}));
}

} // namespace starrocks
