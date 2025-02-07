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

#include "exec/pipeline/schedule/observer.h"

#include <unistd.h>

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

#pragma GCC push_options
#pragma GCC optimize("no-inline")

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
        std::atomic_int32_t sync{};

        std::vector<int> data;
        data.emplace_back(0);
        std::vector<std::jthread> threads;
        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([&]() {
                {
                    std::unique_lock lock(mutex);
                    s.release();
                    cv.wait(lock, [&]() { return sync != 0; });
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
        {
            std::lock_guard guard(mutex);
            sync = 1;
            cv.notify_all();
        }

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
    {
        std::counting_semaphore<> s(0);
        int changed = false;
        struct SleepTimer : public PipelineTimerTask {
            SleepTimer(int32_t& changed_, std::counting_semaphore<>& s_) : changed(changed_), s(s_) {}
            void Run() override {
                s.release();
                (void)sleep(5);
                changed = true;
            }
            int32_t& changed;
            std::counting_semaphore<>& s;
        };
        SleepTimer noop(changed, s);
        //
        timespec abstime = butil::microseconds_to_timespec(butil::gettimeofday_us());
        timespec s1 = abstime;
        s1.tv_sec -= 10;
        // schedule a expired task
        ASSERT_OK(timer.schedule(&noop, s1));
        s.acquire();
        // will wait util timer finished
        noop.unschedule(&timer);
        ASSERT_TRUE(changed);
    }
}

class PipelineObserverTest : public ::testing::Test {
public:
    void SetUp() override {
        _dummy_query_ctx = std::make_shared<QueryContext>();
        _dummy_fragment_ctx = std::make_shared<FragmentContext>();
        _exec_group = std::make_shared<NormalExecutionGroup>();
        _runtime_state = std::make_shared<RuntimeState>();
        _runtime_state->_obj_pool = std::make_shared<ObjectPool>();
        _runtime_state->set_query_ctx(_dummy_query_ctx.get());
        _runtime_state->set_fragment_ctx(_dummy_fragment_ctx.get());
        _runtime_state->_profile = std::make_shared<RuntimeProfile>("dummy");
        _dummy_fragment_ctx->set_runtime_state(std::move(_runtime_state));
        _runtime_state = _dummy_fragment_ctx->runtime_state_ptr();
    }

    std::shared_ptr<QueryContext> _dummy_query_ctx;
    std::shared_ptr<FragmentContext> _dummy_fragment_ctx;
    std::shared_ptr<NormalExecutionGroup> _exec_group;
    std::shared_ptr<RuntimeState> _runtime_state;
};

struct SimpleTestContext {
    SimpleTestContext(OpFactories factories, ExecutionGroup* exec_group, FragmentContext* fragment_ctx,
                      QueryContext* query_ctx)
            : pipeline(0, std::move(factories), exec_group) {
        auto operators = pipeline.create_operators(1, 0);
        driver = std::make_unique<PipelineDriver>(operators, query_ctx, fragment_ctx, &pipeline, 1);
        driver->assign_observer();
        driver_queue = std::make_unique<QuerySharedDriverQueue>(metrics.get_driver_queue_metrics());
        fragment_ctx->init_event_scheduler();
        fragment_ctx->event_scheduler()->attach_queue(driver_queue.get());
    }

    Pipeline pipeline;
    PipelineExecutorMetrics metrics;
    std::unique_ptr<DriverQueue> driver_queue;
    std::unique_ptr<PipelineDriver> driver;
};

TEST_F(PipelineObserverTest, basic_test) {
    OpFactories factories;
    factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));

    for (auto& factory : factories) {
        ASSERT_TRUE(factory->support_event_scheduler());
    }

    SimpleTestContext tx(factories, _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    const auto& driver = tx.driver;
    const auto& driver_queue = tx.driver_queue;

    driver->set_in_blocked(true);
    driver->set_driver_state(DriverState::INPUT_EMPTY);
    // test notify
    driver->observer()->all_trigger();
    ASSERT_OK(driver_queue->take(false));
    driver->observer()->cancel_trigger();
    ASSERT_OK(driver_queue->take(false));
    driver->observer()->sink_trigger();
    ASSERT_OK(driver_queue->take(false));
    driver->observer()->source_trigger();
    driver->observer()->source_trigger();
}

TEST_F(PipelineObserverTest, test_obs) {
    OpFactories factories;
    factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));
    SimpleTestContext tx(factories, _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    const auto& driver = tx.driver;

    driver->set_in_blocked(true);
    driver->set_driver_state(DriverState::PENDING_FINISH);
    Observable obs;
    _runtime_state->set_enable_event_scheduler(true);
    obs.add_observer(_runtime_state.get(), driver->observer());
    ASSERT_GT(obs.to_string().size(), 0);
    obs.notify_sink_observers();
    obs.notify_source_observers();
}

TEST_F(PipelineObserverTest, test_cancel) {
    OpFactories factories;
    factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));

    SimpleTestContext tx(factories, _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    const auto& driver = tx.driver;

    driver->set_driver_state(DriverState::INPUT_EMPTY);
    _dummy_fragment_ctx->cancel(Status::InternalError("error"));
    driver->set_in_blocked(true);
    driver->observer()->all_trigger();
    for (size_t i = 0; i < driver->_operator_stages.size(); ++i) {
        driver->_operator_stages[i] = OperatorStage::CLOSED;
    }
}

TEST_F(PipelineObserverTest, test_add_blocked_driver) {
    OpFactories factories;
    factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));

    SimpleTestContext tx(factories, _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    const auto& driver = tx.driver;

    driver->set_driver_state(DriverState::INPUT_EMPTY);
    _dummy_fragment_ctx->event_scheduler()->add_blocked_driver(driver.get());
}

TEST_F(PipelineObserverTest, race_scheduler_observer) {
    OpFactories factories;
    factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));

    SimpleTestContext tx(factories, _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    const auto& driver = tx.driver;

    driver->set_driver_state(DriverState::INPUT_EMPTY);
    Observable obs;
    obs.add_observer(_runtime_state.get(), driver->observer());

    std::vector<std::jthread> threads;

    threads.emplace_back([&]() { _dummy_fragment_ctx->event_scheduler()->add_blocked_driver(driver.get()); });
    threads.emplace_back([&]() { obs.notify_source_observers(); });

    for (auto& thread : threads) {
        thread.join();
    }
}

} // namespace starrocks::pipeline
#pragma GCC pop_options
