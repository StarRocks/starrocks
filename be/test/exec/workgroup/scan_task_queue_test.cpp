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
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/metrics.h"

namespace starrocks::workgroup {

PARALLEL_TEST(ScanExecutorTest, test_yield) {
    auto queue = std::make_unique<PriorityScanTaskQueue>(100);
    std::unique_ptr<ThreadPool> thread_pool;
    ASSERT_OK(ThreadPoolBuilder("scan_yield")
                      .set_min_threads(0)
                      .set_max_threads(4)
                      .set_max_queue_size(100)
                      .build(&thread_pool));
    pipeline::ScanExecutorMetrics metrics;
    auto executor = std::make_unique<ScanExecutor>(std::move(thread_pool), std::move(queue), &metrics);
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
                std::lock_guard guard(mutex);
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

PARALLEL_TEST(ScanExecutorTest, test_thread_pool_metrics) {
    constexpr int kNumThreads = 4;
    // Declared before the registry so that the registry is destroyed first.
    pipeline::ScanExecutorMetrics metrics;
    MetricRegistry registry("test");
    metrics.thread_pool.register_all_metrics(&registry, "pipe_scan_");

    auto& expected_threads = metrics.thread_pool.expected_worker_threads;
    auto& alive_threads = metrics.thread_pool.worker_threads;
    ASSERT_TRUE(registry.get_metric("pipe_scan_expected_worker_threads") != nullptr);
    ASSERT_TRUE(registry.get_metric("pipe_scan_worker_threads") != nullptr);

    auto make_executor = [&]() {
        std::unique_ptr<ThreadPool> thread_pool;
        CHECK(ThreadPoolBuilder("scan_metrics")
                      .set_min_threads(0)
                      .set_max_threads(kNumThreads)
                      .set_max_queue_size(100)
                      .build(&thread_pool)
                      .ok());
        return std::make_unique<ScanExecutor>(std::move(thread_pool), std::make_unique<PriorityScanTaskQueue>(100),
                                              &metrics);
    };
    // Workers are started asynchronously.
    auto collect_until_alive = [&](uint64_t target) {
        for (int i = 0; i < 500 && alive_threads.value() != target; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            registry.trigger_hook();
        }
    };

    registry.trigger_hook();
    ASSERT_EQ(0, expected_threads.value());
    ASSERT_EQ(0, alive_threads.value());

    {
        auto executor = make_executor();

        // No worker is expected before initialize(), even though the pool can already hold them.
        registry.trigger_hook();
        ASSERT_EQ(0, expected_threads.value());
        ASSERT_EQ(0, alive_threads.value());

        executor->initialize(kNumThreads);
        collect_until_alive(kNumThreads);
        ASSERT_EQ(kNumThreads, expected_threads.value());
        ASSERT_EQ(kNumThreads, alive_threads.value());

        // Shrinking the executor lowers the expectation immediately, so a resource group being
        // resized cannot be mistaken for workers that went missing. The workers themselves only
        // leave once they wake up for a task, so the count of alive ones may lag behind.
        executor->change_num_threads(kNumThreads / 2);
        registry.trigger_hook();
        ASSERT_EQ(kNumThreads / 2, expected_threads.value());
        ASSERT_GE(alive_threads.value(), expected_threads.value());

        executor->close();
        registry.trigger_hook();
        ASSERT_EQ(0, expected_threads.value());
        ASSERT_EQ(0, alive_threads.value());
    }

    // An executor destroyed without close() must stop being sampled as well: a sampler left behind
    // would reach into freed memory on the next collection.
    {
        auto executor = make_executor();
        registry.trigger_hook();
    }
    registry.trigger_hook();
    ASSERT_EQ(0, expected_threads.value());
    ASSERT_EQ(0, alive_threads.value());

    // Every executor contributes, as a resource group with exclusive executors has its own.
    auto first = make_executor();
    auto second = make_executor();
    first->initialize(kNumThreads);
    second->initialize(kNumThreads);
    collect_until_alive(2 * kNumThreads);
    ASSERT_EQ(2 * kNumThreads, expected_threads.value());
    ASSERT_EQ(2 * kNumThreads, alive_threads.value());
    first->close();
    second->close();
    registry.trigger_hook();
    ASSERT_EQ(0, expected_threads.value());
    ASSERT_EQ(0, alive_threads.value());
}

} // namespace starrocks::workgroup
