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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "compute_env/pipeline/pipeline_timer.h"
#include "compute_env/pipeline/pipeline_timer_context.h"
#include "exec/exec_env.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/runtime/fragment_context.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/query_context.h"
#include "exec/runtime/schedule/event_scheduler.h"
#include "exec_primitive/pipeline/primitives/driver_queue.h"
#include "exec_primitive/pipeline/primitives/driver_state.h"
#include "exec_primitive/pipeline/primitives/pipeline_metrics.h"
#include "exec_primitive/pipeline/runtime_filter_hub.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

constexpr int64_t kGlobalRfWaitMs = 100;
constexpr int64_t kLocalRfWaitMs = 600;

// A real driver with an empty-set source and a noop sink. The tests drive is_precondition_block() directly and
// let the real pipeline timer wake the driver, so the operators are never run.
struct RfTimerDriverContext {
    RfTimerDriverContext(OpFactories factories, ExecutionGroup* exec_group, FragmentContext* fragment_ctx,
                         QueryContext* query_ctx, PipelineTimerContextPtr timer_context)
            : pipeline(0, std::move(factories), exec_group) {
        auto operators = pipeline.create_operators(1, 0);
        driver = std::make_unique<PipelineDriver>(operators, &query_ctx->query_runtime_state(),
                                                  &fragment_ctx->fragment_runtime_state(), pipeline.pipeline_event(),
                                                  &pipeline, std::move(timer_context), 1);
        driver->set_observer(fragment_ctx->event_scheduler()->create_driver_observer(driver.get()));
        driver->assign_observer();
        driver_queue = std::make_unique<QuerySharedDriverQueue>(metrics.get_driver_queue_metrics());
        fragment_ctx->event_scheduler()->attach_queue(driver_queue.get());
    }

    Pipeline pipeline;
    PipelineExecutorMetrics metrics;
    std::unique_ptr<DriverQueue> driver_queue;
    // The driver unschedules its timer on destruction, so it must go before the timer.
    std::unique_ptr<PipelineDriver> driver;
};

} // namespace

class GlobalRfWaitTimerTest : public ::testing::Test {
public:
    void SetUp() override {
        ASSERT_OK(_timer.start());
        _timer_context = std::make_shared<PipelineTimerContext>(&_timer);

        _query_ctx = std::make_shared<QueryContext>();
        _fragment_ctx = std::make_shared<FragmentContext>();
        _exec_group = std::make_shared<NormalExecutionGroup>();

        auto runtime_state = std::make_shared<RuntimeState>();
        auto* exec_env = ExecEnv::GetInstance();
        runtime_state->set_exec_env(exec_env);
        runtime_state->set_query_execution_services(&exec_env->query_execution_services());
        runtime_state->_obj_pool = std::make_shared<ObjectPool>();
        _query_ctx->attach_to_runtime_state(runtime_state.get());
        runtime_state->set_fragment_ctx(_fragment_ctx.get(), &_fragment_ctx->fragment_runtime_state());
        runtime_state->set_fragment_dict_state(_fragment_ctx->dict_state());
        runtime_state->_profile = std::make_shared<RuntimeProfile>("dummy");
        _fragment_ctx->set_runtime_state(std::move(runtime_state));
        _runtime_state = _fragment_ctx->runtime_state();
        _fragment_ctx->init_event_scheduler();
    }

    std::unique_ptr<RfTimerDriverContext> make_driver() {
        OpFactories factories;
        factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
        factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));
        return std::make_unique<RfTimerDriverContext>(std::move(factories), _exec_group.get(), _fragment_ctx.get(),
                                                      _query_ctx.get(), _timer_context);
    }

protected:
    // Declared first so that it is destroyed last: the timer context unschedules its tasks from it.
    PipelineTimer _timer;
    PipelineTimerContextPtr _timer_context;
    std::shared_ptr<QueryContext> _query_ctx;
    std::shared_ptr<FragmentContext> _fragment_ctx;
    std::shared_ptr<NormalExecutionGroup> _exec_group;
    RuntimeState* _runtime_state = nullptr;
};

// The driver waits kLocalRfWaitMs for a local runtime filter and then waits for a global runtime filter that never
// arrives. Under the event scheduler the timer is the only wakeup, so we expect it to fire kGlobalRfWaitMs after
// the local wait ends. A later wakeup leaves the driver asleep after its wait is over, so the test
// requires the timer to fire well before kGlobalRfWaitMs + kLocalRfWaitMs.
TEST_F(GlobalRfWaitTimerTest, timer_fires_at_deadline_after_local_rf_wait) {
    auto ctx = make_driver();
    auto& driver = *ctx->driver;
    ASSERT_OK(driver.prepare(_runtime_state));
    ASSERT_OK(driver.prepare_local_state(_runtime_state));
    _runtime_state->set_enable_event_scheduler(true);

    RuntimeFilterProbeDescriptor global_rf;
    global_rf._is_local = false;
    RuntimeFilterHolder local_rf;
    driver._global_rf_descriptors.push_back(&global_rf);
    driver._global_rf_wait_timeout_ns = kGlobalRfWaitMs * 1000 * 1000;
    driver._all_global_rf_ready_or_timeout = false;
    driver._local_rf_holders.push_back(&local_rf);
    driver._all_local_rf_ready = false;

    driver.start_timers();
    ASSERT_TRUE(driver.is_precondition_block());
    driver.set_driver_state(DriverState::PRECONDITION_BLOCK);
    ASSERT_FALSE(driver.need_check_reschedule());

    std::this_thread::sleep_for(std::chrono::milliseconds(kLocalRfWaitMs));
    local_rf.set_collector(std::make_unique<RuntimeFilterCollector>(RuntimeInFilterList{}));

    // The local wait is over, so the driver now waits for the global runtime filter.
    const auto begin = std::chrono::steady_clock::now();
    ASSERT_TRUE(driver.is_precondition_block());

    // The timer thread sets need_check_reschedule through the driver's observer when it fires. The loop is bounded
    // so that a timer set too late fails the assertions below instead of hanging the test.
    const auto give_up = begin + std::chrono::milliseconds(kGlobalRfWaitMs + kLocalRfWaitMs + 2000);
    while (!driver.need_check_reschedule() && std::chrono::steady_clock::now() < give_up) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto fired_after =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();

    ASSERT_TRUE(driver.need_check_reschedule());
    EXPECT_GE(fired_after, kGlobalRfWaitMs / 2);
    EXPECT_LT(fired_after, kGlobalRfWaitMs + kLocalRfWaitMs / 2);

    // The timeout still counts the local wait, and the driver stops waiting once the timer has fired.
    EXPECT_GE(driver._global_rf_wait_timeout_ns, (kGlobalRfWaitMs + kLocalRfWaitMs) * 1000 * 1000);
    EXPECT_FALSE(driver.is_precondition_block());
}

} // namespace starrocks::pipeline
