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

#include "compute_env/spill/spill_observable.h"

#include <atomic>
#include <memory>
#include <thread>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/primitives/pipeline_metrics.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/schedule/event_scheduler.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {

using namespace starrocks::pipeline;

namespace {

// A minimal PipelineObserver that records which trigger fired. Lets the layer
// be exercised without a full EventScheduler/driver round-trip in the simple
// cases; the driver-backed harness below covers the real dispatch path.
class CountingObserver final : public PipelineObserver {
public:
    void source_trigger() override { source_count.fetch_add(1, std::memory_order_relaxed); }
    void sink_trigger() override { sink_count.fetch_add(1, std::memory_order_relaxed); }
    void cancel_trigger() override { cancel_count.fetch_add(1, std::memory_order_relaxed); }
    void all_trigger() override {
        source_trigger();
        sink_trigger();
    }
    void runtime_filter_timeout_trigger() override {}
    std::string debug_string() const override { return "CountingObserver"; }

    std::atomic_int32_t source_count{0};
    std::atomic_int32_t sink_count{0};
    std::atomic_int32_t cancel_count{0};
};

} // namespace

class SpillObservableTest : public ::testing::Test {
public:
    void SetUp() override {
        _dummy_query_ctx = std::make_shared<QueryContext>();
        _dummy_fragment_ctx = std::make_shared<FragmentContext>();
        _exec_group = std::make_shared<NormalExecutionGroup>();
        _runtime_state = std::make_shared<RuntimeState>();
        auto* exec_env = ExecEnv::GetInstance();
        _runtime_state->set_exec_env(exec_env);
        _runtime_state->set_query_execution_services(&exec_env->query_execution_services());
        _runtime_state->_obj_pool = std::make_shared<ObjectPool>();
        _dummy_query_ctx->attach_to_runtime_state(_runtime_state.get());
        _runtime_state->set_fragment_ctx(_dummy_fragment_ctx.get(), &_dummy_fragment_ctx->fragment_runtime_state());
        _runtime_state->set_fragment_dict_state(_dummy_fragment_ctx->dict_state());
        _runtime_state->_profile = std::make_shared<RuntimeProfile>("dummy");
        _dummy_fragment_ctx->set_runtime_state(std::move(_runtime_state));
        _runtime_state = _dummy_fragment_ctx->runtime_state_ptr();
    }

    std::shared_ptr<QueryContext> _dummy_query_ctx;
    std::shared_ptr<FragmentContext> _dummy_fragment_ctx;
    std::shared_ptr<NormalExecutionGroup> _exec_group;
    std::shared_ptr<RuntimeState> _runtime_state;
};

namespace {

// Mirrors observer_test.cpp's SimpleTestContext: a real PipelineDriver wired to
// an EventScheduler + driver queue so the observer's trigger actually re-queues
// the driver.
struct DriverTestContext {
    DriverTestContext(OpFactories factories, ExecutionGroup* exec_group, FragmentContext* fragment_ctx,
                      QueryContext* query_ctx)
            : pipeline(0, std::move(factories), exec_group) {
        auto operators = pipeline.create_operators(1, 0);
        auto* query_runtime_state = query_ctx == nullptr ? nullptr : &query_ctx->query_runtime_state();
        auto* fragment_runtime_state = fragment_ctx == nullptr ? nullptr : &fragment_ctx->fragment_runtime_state();
        driver = std::make_unique<PipelineDriver>(operators, query_runtime_state, fragment_runtime_state,
                                                  pipeline.pipeline_event(), &pipeline, nullptr, 1);
        driver_queue = std::make_unique<QuerySharedDriverQueue>(metrics.get_driver_queue_metrics());
        fragment_ctx->init_event_scheduler();
        fragment_ctx->event_scheduler()->attach_queue(driver_queue.get());
        driver->set_observer(fragment_ctx->event_scheduler()->create_driver_observer(driver.get()));
        driver->assign_observer();
    }

    Pipeline pipeline;
    PipelineExecutorMetrics metrics;
    std::unique_ptr<DriverQueue> driver_queue;
    std::unique_ptr<PipelineDriver> driver;
};

OpFactories make_factories() {
    OpFactories factories;
    factories.emplace_back(std::make_shared<EmptySetOperatorFactory>(0, 1));
    factories.emplace_back(std::make_shared<NoopSinkOperatorFactory>(2, 3));
    return factories;
}

} // namespace

// Subscribe gate: with the event scheduler disabled, nothing is registered.
TEST_F(SpillObservableTest, gate_disabled_drops_subscription) {
    CountingObserver obs;
    _runtime_state->set_enable_event_scheduler(false);

    SpillEventObservable observable;
    observable.subscribe_sink(_runtime_state.get(), &obs);
    observable.subscribe_source(_runtime_state.get(), &obs);
    ASSERT_EQ(observable.observer_count(), 0);

    observable.notify_sink_observers();
    observable.notify_source_observers();
    ASSERT_EQ(obs.sink_count.load(), 0);
    ASSERT_EQ(obs.source_count.load(), 0);
}

// Routing: the sink list is woken with sink_trigger only, the source list with
// source_trigger only.
TEST_F(SpillObservableTest, notify_routes_by_list) {
    CountingObserver sink_obs;
    CountingObserver source_obs;
    _runtime_state->set_enable_event_scheduler(true);

    SpillEventObservable observable;
    observable.subscribe_sink(_runtime_state.get(), &sink_obs);
    observable.subscribe_source(_runtime_state.get(), &source_obs);
    ASSERT_EQ(observable.observer_count(), 2);

    observable.notify_sink_observers();
    ASSERT_EQ(sink_obs.sink_count.load(), 1);
    ASSERT_EQ(sink_obs.source_count.load(), 0);
    ASSERT_EQ(source_obs.source_count.load(), 0);

    observable.notify_source_observers();
    ASSERT_EQ(source_obs.source_count.load(), 1);
    ASSERT_EQ(source_obs.sink_count.load(), 0);
    ASSERT_EQ(sink_obs.source_count.load(), 0);
}

// detach clears both lists; subsequent notify is a no-op.
TEST_F(SpillObservableTest, detach_clears_both_lists) {
    CountingObserver obs;
    _runtime_state->set_enable_event_scheduler(true);

    SpillEventObservable observable;
    observable.subscribe_sink(_runtime_state.get(), &obs);
    observable.subscribe_source(_runtime_state.get(), &obs);
    ASSERT_EQ(observable.observer_count(), 2);

    observable.detach_observers();
    ASSERT_EQ(observable.observer_count(), 0);

    observable.notify_sink_observers();
    observable.notify_source_observers();
    ASSERT_EQ(obs.sink_count.load(), 0);
    ASSERT_EQ(obs.source_count.load(), 0);
}

// A real driver blocked as INPUT_EMPTY is re-queued when the source list fires.
TEST_F(SpillObservableTest, source_trigger_requeues_real_driver) {
    DriverTestContext tx(make_factories(), _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    ASSERT_OK(tx.driver->prepare_local_state(_runtime_state.get()));

    SpillEventObservable observable;
    _runtime_state->set_enable_event_scheduler(true);
    observable.subscribe_source(_runtime_state.get(), tx.driver->observer());
    ASSERT_EQ(observable.observer_count(), 1);

    tx.driver->set_in_blocked(true);
    tx.driver->set_driver_state(DriverState::INPUT_EMPTY);
    observable.notify_source_observers();
    ASSERT_OK(tx.driver_queue->take(false));
}

// A real driver blocked as OUTPUT_FULL is re-queued when the sink list fires.
TEST_F(SpillObservableTest, sink_trigger_requeues_real_driver) {
    DriverTestContext tx(make_factories(), _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    ASSERT_OK(tx.driver->prepare_local_state(_runtime_state.get()));

    SpillEventObservable observable;
    _runtime_state->set_enable_event_scheduler(true);
    observable.subscribe_sink(_runtime_state.get(), tx.driver->observer());

    tx.driver->set_in_blocked(true);
    tx.driver->set_driver_state(DriverState::OUTPUT_FULL);
    observable.notify_sink_observers();
    ASSERT_OK(tx.driver_queue->take(false));
}

// detach during an in-flight notify must not crash: notify holds the shared
// lock, detach the exclusive one (TSAN-relevant, jthread pattern from
// race_scheduler_observer).
TEST_F(SpillObservableTest, detach_notify_race) {
    DriverTestContext tx(make_factories(), _exec_group.get(), _dummy_fragment_ctx.get(), _dummy_query_ctx.get());
    ASSERT_OK(tx.driver->prepare(_runtime_state.get()));
    ASSERT_OK(tx.driver->prepare_local_state(_runtime_state.get()));

    SpillEventObservable observable;
    _runtime_state->set_enable_event_scheduler(true);
    observable.subscribe_source(_runtime_state.get(), tx.driver->observer());

    tx.driver->set_driver_state(DriverState::INPUT_EMPTY);

    std::vector<std::jthread> threads;
    threads.emplace_back([&]() { observable.notify_source_observers(); });
    threads.emplace_back([&]() { observable.detach_observers(); });
    for (auto& thread : threads) {
        thread.join();
    }
}

} // namespace starrocks::spill
