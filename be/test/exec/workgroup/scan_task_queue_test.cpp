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
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

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

} // namespace starrocks::workgroup
