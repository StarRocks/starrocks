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

#include <memory>
#include <vector>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/primitives/driver_queue.h"
#include "exec/pipeline/primitives/driver_state.h"
#include "exec/pipeline/primitives/event.h"
#include "exec/pipeline/schedule/event_scheduler.h"
#include "exec/runtime/fragment_runtime_state.h"
#include "exec/runtime/query_runtime_state.h"
#include "runtime/query_context_lifetime.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

class TestSourceOperator final : public SourceOperator {
public:
    TestSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "test_source", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return has_output_value; }
    bool is_finished() const override { return is_finished_value; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }

    bool has_output_value = false;
    bool is_finished_value = false;
};

class TestSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestSourceOperatorFactory() : SourceOperatorFactory(1, "test_source", 10) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSourceOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

class TestSinkOperator final : public Operator {
public:
    TestSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "test_sink", plan_node_id, false, driver_sequence) {}

    bool has_output() const override { return false; }
    bool need_input() const override { return need_input_value; }
    bool is_finished() const override { return is_finished_value; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }

    bool need_input_value = true;
    bool is_finished_value = false;
};

class TestSinkOperatorFactory final : public OperatorFactory {
public:
    TestSinkOperatorFactory() : OperatorFactory(2, "test_sink", 20) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSinkOperator>(this, id(), plan_node_id(), driver_sequence);
    }
};

class RecordingDriverQueue final : public DriverQueue {
public:
    RecordingDriverQueue() : DriverQueue(nullptr) {}

    void close() override { is_closed = true; }

    void put_back(const DriverRawPtr driver) override {
        queued_drivers.emplace_back(driver);
        driver->set_in_ready(true);
        driver->set_in_queue(this);
    }

    void put_back(const std::vector<DriverRawPtr>& drivers) override {
        for (auto* driver : drivers) {
            put_back(driver);
        }
    }

    void put_back_from_executor(const DriverRawPtr driver) override { put_back(driver); }

    StatusOr<DriverRawPtr> take(const bool block) override {
        if (queued_drivers.empty()) {
            return Status::InternalError("empty driver queue");
        }
        auto* driver = queued_drivers.front();
        queued_drivers.erase(queued_drivers.begin());
        driver->set_in_ready(false);
        return driver;
    }

    void cancel(DriverRawPtr driver) override {}
    void update_statistics(const DriverRawPtr driver) override {}
    size_t size() const override { return queued_drivers.size(); }
    bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override { return false; }

    bool is_closed = false;
    std::vector<DriverRawPtr> queued_drivers;
};

struct SchedulerHarness {
    SchedulerHarness()
            : source(std::static_pointer_cast<TestSourceOperator>(source_factory.create(1, 0))),
              sink(std::static_pointer_cast<TestSinkOperator>(sink_factory.create(1, 0))) {
        operators.emplace_back(source);
        operators.emplace_back(sink);
        driver = std::make_unique<PipelineDriver>(operators, &query_runtime_state, &fragment_runtime_state,
                                                  pipeline_event.get(), nullptr, nullptr, -1);
    }

    Status prepare() {
        runtime_state._obj_pool = std::make_shared<ObjectPool>();
        runtime_state.set_query_runtime_state(&query_runtime_state);
        runtime_state.set_fragment_runtime_state(&fragment_runtime_state);
        runtime_state.set_query_ctx_lifetime(query_lifetime);
        runtime_state.set_enable_event_scheduler(true);

        event_scheduler.attach_queue(&driver_queue);
        driver->set_observer(event_scheduler.create_driver_observer(driver.get()));
        driver->assign_observer();

        RETURN_IF_ERROR(driver->prepare(&runtime_state));
        return driver->prepare_local_state(&runtime_state);
    }

    void make_driver_input_ready() {
        source->has_output_value = true;
        sink->need_input_value = true;
    }

    void mark_driver_input_empty() { driver->set_driver_state(DriverState::INPUT_EMPTY); }

    TestSourceOperatorFactory source_factory;
    TestSinkOperatorFactory sink_factory;
    QueryRuntimeState query_runtime_state;
    FragmentRuntimeState fragment_runtime_state;
    RuntimeState runtime_state{TQueryGlobals{}};
    std::shared_ptr<QueryContextLifetime> query_lifetime = std::make_shared<QueryContextLifetime>();
    EventPtr pipeline_event = Event::create_event();
    Operators operators;
    std::shared_ptr<TestSourceOperator> source;
    std::shared_ptr<TestSinkOperator> sink;
    EventScheduler event_scheduler;
    RecordingDriverQueue driver_queue;
    std::unique_ptr<PipelineDriver> driver;
};

TEST(EventSchedulerExecRuntimeTest, ObserverUsesInjectedSchedulerWithoutFragmentContext) {
    SchedulerHarness harness;
    ASSERT_OK(harness.prepare());
    ASSERT_EQ(nullptr, harness.runtime_state.query_ctx());
    ASSERT_EQ(nullptr, harness.runtime_state.fragment_ctx());

    harness.make_driver_input_ready();
    harness.mark_driver_input_empty();
    harness.driver->set_in_blocked(true);

    harness.driver->observer()->source_trigger();

    ASSERT_EQ(1, harness.driver_queue.queued_drivers.size());
    EXPECT_EQ(harness.driver.get(), harness.driver_queue.queued_drivers.back());
    EXPECT_FALSE(harness.driver->is_in_blocked());
    EXPECT_EQ(DriverState::READY, harness.driver->driver_state());
}

TEST(EventSchedulerExecRuntimeTest, AddBlockedDriverUsesLifetimeTokenWithoutQueryContext) {
    SchedulerHarness harness;
    ASSERT_OK(harness.prepare());
    ASSERT_EQ(nullptr, harness.runtime_state.query_ctx());
    ASSERT_EQ(nullptr, harness.runtime_state.fragment_ctx());

    harness.make_driver_input_ready();
    harness.mark_driver_input_empty();

    harness.event_scheduler.add_blocked_driver(harness.driver.get());

    ASSERT_EQ(1, harness.driver_queue.queued_drivers.size());
    EXPECT_EQ(harness.driver.get(), harness.driver_queue.queued_drivers.back());
    EXPECT_FALSE(harness.driver->is_in_blocked());
    EXPECT_EQ(DriverState::READY, harness.driver->driver_state());
}

} // namespace
} // namespace starrocks::pipeline
