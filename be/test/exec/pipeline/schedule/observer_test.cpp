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

#include <bits/types/struct_timespec.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "butil/time.h"
#include "common/object_pool.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/schedule/pipeline_timer.h"
#include "exec/pipeline/schedule/utils.h"
#include "gtest/gtest.h"
#include "testutil/assert.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {
TEST(RequestCntlTest, test) {
    {
        std::atomic_int32_t val{};
        std::vector<int> data;
        for (size_t i = 0; i < 10; ++i) {
            data.emplace_back(i);
        }
        int loop = 0;
        AtomicRequestControler cntl(val, [&]() {
            loop++;
            while (!data.empty()) {
                data.pop_back();
            }
        });
        ASSERT_EQ(loop, 1);
    }
    {
        // test run with concurrency
        std::mutex mutex;
        std::condition_variable cv;
        std::atomic_int32_t val{};
        std::counting_semaphore<> s(0);
        std::vector<int> data;
        data.emplace_back(0);
        std::vector<std::jthread> threads;
        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([&]() {
                {
                    std::unique_lock lock(mutex);
                    s.release();
                    cv.wait(lock);
                }
                AtomicRequestControler cntl(val, [&]() {
                    while (!data.empty()) {
                        data.pop_back();
                    }
                });
            });
        }
        for (size_t i = 0; i < 10; ++i) {
            s.acquire();
        }
        cv.notify_all();
        for (auto& thread : threads) {
            thread.join();
        }
        ASSERT_EQ(data.size(), 0);
    }
}

TEST(TimerThreadTest, test) {
    PipelineTimer timer;
    ASSERT_OK(timer.start());
    {
        std::counting_semaphore<> s(0);
        int changed = false;
        struct Timer : public PipelineTimerTask {
            Timer(int32_t& changed_, std::counting_semaphore<>& s_) : changed(changed_), s(s_) {}
            void Run() override {
                changed = true;
                s.release();
            }
            int32_t& changed;
            std::counting_semaphore<>& s;
        };
        Timer noop(changed, s);
        //
        timespec abstime = butil::microseconds_to_timespec(butil::gettimeofday_us());
        timespec s1 = abstime;
        s1.tv_sec -= 10;
        // schedule a expired task
        ASSERT_OK(timer.schedule(&noop, s1));
        s.acquire();
        noop.unschedule(&timer);
        ASSERT_TRUE(changed);

        timespec s2 = abstime;
        s2.tv_sec += 3600;
        // schedule a task
        changed = false;
        ASSERT_OK(timer.schedule(&noop, s2));
        sleep(1);
        noop.unschedule(&timer);
        ASSERT_FALSE(changed);
    }
}

TEST(ObservableTest, test) {
    auto dummy_query_ctx = std::make_shared<QueryContext>();
    auto dummy_fragment_ctx = std::make_shared<FragmentContext>();
    auto exec_group = std::make_shared<NormalExecutionGroup>();
    auto runtime_state = std::make_shared<RuntimeState>();
    runtime_state->_obj_pool = std::make_shared<ObjectPool>();
    runtime_state->set_query_ctx(dummy_query_ctx.get());
    runtime_state->set_fragment_ctx(dummy_fragment_ctx.get());
    runtime_state->_profile = std::make_shared<RuntimeProfile>("dummy");
    dummy_fragment_ctx->set_runtime_state(std::move(runtime_state));
    OpFactories factories;
    factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));

    Pipeline pipeline(0, factories, exec_group.get());
    auto operators = pipeline.create_operators(1, 0);
    PipelineDriver driver(operators, dummy_query_ctx.get(), dummy_fragment_ctx.get(), &pipeline, 1);
    driver.assign_observer();
    ASSERT_OK(driver.prepare(dummy_fragment_ctx->runtime_state()));

    auto driver_queue = std::make_unique<QuerySharedDriverQueue>();
    dummy_fragment_ctx->init_event_scheduler();
    dummy_fragment_ctx->event_scheduler()->attach_queue(driver_queue.get());

    driver.set_in_block_queue(true);
    driver.set_driver_state(DriverState::INPUT_EMPTY);
    // test notify
    driver.observer()->all_update();
    ASSERT_OK(driver_queue->take(false));
    driver.observer()->cancel_update();
    ASSERT_OK(driver_queue->take(false));
    driver.observer()->sink_update();
    ASSERT_OK(driver_queue->take(false));
    driver.observer()->source_update();
    driver.observer()->source_update();
}
} // namespace starrocks::pipeline