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

#include "exec/workgroup/scan_task_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks::workgroup {

PARALLEL_TEST(MultiLevelFeedScanTaskQueueTest, test_basic) {
    MultiLevelFeedScanTaskQueue queue;

    // Prepare tasks.
    std::shared_ptr<ScanTaskGroup> group7 = std::make_shared<ScanTaskGroup>();
    group7->sub_queue_level = 7;
    std::shared_ptr<ScanTaskGroup> group6 = std::make_shared<ScanTaskGroup>();
    group6->sub_queue_level = 6;
    std::shared_ptr<ScanTaskGroup> group5 = std::make_shared<ScanTaskGroup>();
    group5->sub_queue_level = 5;

    ScanTask task71;
    task71.priority = 71;
    task71.task_group = group7;
    queue.update_statistics(task71, 5'000'000L * 1);

    ScanTask task72;
    task72.priority = 72;
    task72.task_group = group7;
    queue.update_statistics(task72, 5'000'000L * 1);

    ScanTask task61;
    task61.priority = 61;
    task61.task_group = group6;
    queue.update_statistics(task61, 30'000'000L * MultiLevelFeedScanTaskQueue::ratio_of_adjacent_queue());

    ScanTask task51;
    task51.priority = 51;
    task51.task_group = group5;
    queue.update_statistics(task51, 20'000'000L * MultiLevelFeedScanTaskQueue::ratio_of_adjacent_queue() *
                                            MultiLevelFeedScanTaskQueue::ratio_of_adjacent_queue());

    // Use task.priority as the unique index of task.
    std::vector<int> out_task_priorities = {task71.priority, task72.priority, task51.priority, task61.priority};
    std::vector<ScanTask> in_tasks;
    in_tasks.emplace_back(std::move(task71));
    in_tasks.emplace_back(std::move(task72));
    in_tasks.emplace_back(std::move(task61));
    in_tasks.emplace_back(std::move(task51));

    // Put back drivers to queue.
    for (auto& task : in_tasks) {
        ASSERT_TRUE(queue.try_offer(std::move(task)));
    }

    // Take drivers from queue.
    for (int out_task_priority : out_task_priorities) {
        auto maybe_task = queue.take();
        ASSERT_TRUE(maybe_task.ok());
        ASSERT_EQ(out_task_priority, maybe_task->priority);
    }

    ASSERT_EQ(1, group7.use_count());
    ASSERT_EQ(1, group6.use_count());
    ASSERT_EQ(1, group5.use_count());
}

PARALLEL_TEST(MultiLevelFeedScanTaskQueueTest, test_take_block) {
    MultiLevelFeedScanTaskQueue queue;

    // Prepare tasks.
    std::shared_ptr<ScanTaskGroup> group1 = std::make_shared<ScanTaskGroup>();
    group1->sub_queue_level = 1;
    ScanTask task1;
    task1.priority = 1;
    task1.task_group = group1;

    std::thread consumer_thread([&queue] {
        auto maybe_task = queue.take();
        ASSERT_TRUE(maybe_task.ok());
        ASSERT_EQ(1, maybe_task.value().priority);
    });

    sleep(1);
    queue.update_statistics(task1, 5'000'000L * 1);
    ASSERT_TRUE(queue.try_offer(std::move(task1)));

    consumer_thread.join();

    ASSERT_EQ(1, group1.use_count());
}

PARALLEL_TEST(MultiLevelFeedScanTaskQueueTest, test_take_close) {
    MultiLevelFeedScanTaskQueue queue;

    std::thread consumer_thread([&queue] {
        auto maybe_task = queue.take();
        ASSERT_TRUE(maybe_task.status().is_cancelled());
    });

    sleep(1);
    queue.close();

    consumer_thread.join();
}

PARALLEL_TEST(ScanExecutorTest, test_yield) {
    auto queue = std::make_unique<PriorityScanTaskQueue>(100);
    std::unique_ptr<ThreadPool> thread_pool;
    ASSERT_OK(ThreadPoolBuilder("scan_yield")
                      .set_min_threads(0)
                      .set_max_threads(4)
                      .set_max_queue_size(100)
                      .build(&thread_pool));
    auto executor = std::make_unique<ScanExecutor>(std::move(thread_pool), std::move(queue), false);
    DeferOp op([&]() { executor->close(); });
    executor->initialize(4);

    std::promise<int> a;
    std::string res;
    ScanTask scan_task([&](auto& ctx) {
        ctx.total_yield_point_cnt = 4;
        DCHECK_LT(ctx.yield_point, ctx.total_yield_point_cnt);
        switch (ctx.yield_point) {
        case 0:
            ctx.yield_point++;
            res += "0";
            return;
        case 1:
            ctx.yield_point++;
            res += "1";
            return;
        case 2:
            ctx.yield_point++;
            res += "2";
            return;
        case 3:
            ctx.yield_point++;
            res += "3";
            a.set_value(1);
            return;
        }
    });

    ASSERT_TRUE(executor->submit(std::move(scan_task)));
    a.get_future().get();
    ASSERT_EQ(res, "0123");

    // test overloaded
    std::atomic_int finished_tasks = 0;
    size_t submit_tasks = 0;
    std::mutex mutex;
    std::condition_variable cv;
    for (size_t i = 0; i < 100; ++i) {
        ScanTask overload_task([&](auto& ctx) {
            ctx.total_yield_point_cnt = 2;
            DCHECK_LT(ctx.yield_point, ctx.total_yield_point_cnt);
            if (ctx.yield_point == 1) {
                finished_tasks++;
                cv.notify_one();
            }
            ctx.yield_point++;
        });
        submit_tasks += executor->submit(std::move(overload_task));
    }
    std::unique_lock lock(mutex);
    cv.wait(lock, [&]() { return submit_tasks == finished_tasks.load(); });
    ASSERT_EQ(submit_tasks, finished_tasks.load());
}

} // namespace starrocks::workgroup
