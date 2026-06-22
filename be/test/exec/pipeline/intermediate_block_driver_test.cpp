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

// Real-PipelineDriver coverage for INTERMEDIATE_BLOCK and the cancel release.
//
// Built on the observer_test.cpp / exec_group_test.cpp precedent: a real QueryContext + FragmentContext +
// RuntimeState + ExecutionGroup, a real Pipeline, and a real PipelineDriver built from real
// OperatorFactory::create() and run through driver->prepare(), prepare_local_state() and process(). No
// copies of the predicates: the driver core's own classification (process()), release
// (check_is_ready/is_not_blocked -> _has_intermediate_block) and cancel-release (EventScheduler::
// try_schedule, the released-first is_cancelled branch) all run against lightweight mock operators that are
// real Operator/SourceOperator subclasses.
//
// The operator chain modelled is [source(has_output) -> interior -> sink(need_input)], which is exactly a
// spilled hash-join probe sitting interior to its edges: the interior is the wakeable operator, the
// blocking predicate is an interior need_input() == false (build-side load latch not ready), and the
// release is a notify that flips that predicate.

#include <gtest/gtest.h>

#include <atomic>
#include <memory>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/primitives/driver_state.h"
#include "exec/pipeline/primitives/pipeline_metrics.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/schedule/event_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

namespace {

// A source that is always open (has_output()==true, never finished). Its has_output() being true keeps
// the source edge off the INPUT_EMPTY classification, so a block downstream is strictly interior.
// pull_chunk is never reached in these tests (the first pair stalls on the interior's need_input).
class MockOpenSourceOperator final : public SourceOperator {
public:
    MockOpenSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "mock_open_source", plan_node_id, false, driver_sequence) {}
    ~MockOpenSourceOperator() override = default;

    bool has_output() const override { return true; }
    bool is_finished() const override { return false; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::InternalError("mock source pull_chunk should not be reached");
    }
};

class MockOpenSourceOperatorFactory final : public SourceOperatorFactory {
public:
    MockOpenSourceOperatorFactory(int32_t id, int32_t plan_node_id)
            : SourceOperatorFactory(id, "mock_open_source", plan_node_id) {}
    ~MockOpenSourceOperatorFactory() override = default;

    bool support_event_scheduler() const override { return true; }
    AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MockOpenSourceOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

// An interior (non-edge) operator that models the spilled probe: it can be parked on an interior wait
// (need_input()==false, e.g. build-side load latch not ready, BlockReason::WAIT_LATCH) and, depending on
// the construction flag, either declares supports_intermediate_wakeup()==true (wakeable -> the driver may
// park it in INTERMEDIATE_BLOCK) or false (plain interior -> the driver stays READY). need_input is an
// atomic so a "notify" (test thread) can flip the predicate to release the parked driver.
class MockInteriorOperator final : public Operator {
public:
    MockInteriorOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                         bool wakeable)
            : Operator(factory, id, "mock_interior", plan_node_id, false, driver_sequence), _wakeable(wakeable) {}
    ~MockInteriorOperator() override = default;

    // Interior producer: has output only once it has been fed; starts with nothing to emit. has_output()
    // false combined with not finished is one of the two interior-block triggers in process().
    bool has_output() const override { return _has_output.load(); }
    // The interior block under test: closed until a notify opens it.
    bool need_input() const override { return _need_input.load(); }
    bool is_finished() const override { return _is_finished.load(); }

    bool supports_intermediate_wakeup() const override { return _wakeable; }
    // Named only while actually parked (need_input closed). A runnable interior returns NONE, matching the
    // operator-surface contract verify_block_reason_covered() relies on.
    BlockReason block_reason() const override {
        if (_wakeable && !_need_input.load()) {
            return BlockReason::WAIT_LATCH;
        }
        return BlockReason::NONE;
    }
    uint32_t covered_wakeups() const override { return _wakeable ? block_reason_bit(BlockReason::WAIT_LATCH) : 0u; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::InternalError("mock interior pull_chunk should not be reached");
    }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }

    // Test-thread "notify": flip the parked predicate, the way a build-side load latch completion would.
    void open_input() { _need_input.store(true); }

private:
    const bool _wakeable;
    std::atomic<bool> _need_input{false};
    std::atomic<bool> _has_output{false};
    std::atomic<bool> _is_finished{false};
};

class MockInteriorOperatorFactory final : public OperatorFactory {
public:
    MockInteriorOperatorFactory(int32_t id, int32_t plan_node_id, bool wakeable)
            : OperatorFactory(id, "mock_interior", plan_node_id), _wakeable(wakeable) {}
    ~MockInteriorOperatorFactory() override = default;

    bool support_event_scheduler() const override { return true; }
    bool supports_intermediate_wakeup() const override { return _wakeable; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MockInteriorOperator>(this, _id, _plan_node_id, driver_sequence, _wakeable);
    }

private:
    const bool _wakeable;
};

// A sink that always needs input and is never finished -- keeps the sink edge off the OUTPUT_FULL
// classification so any block in process() is strictly interior.
class MockHungrySinkOperator final : public Operator {
public:
    MockHungrySinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "mock_hungry_sink", plan_node_id, true, driver_sequence) {}
    ~MockHungrySinkOperator() override = default;

    bool need_input() const override { return true; }
    bool has_output() const override { return false; }
    bool is_finished() const override { return false; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::InternalError("mock sink pull_chunk should not be reached");
    }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

class MockHungrySinkOperatorFactory final : public OperatorFactory {
public:
    MockHungrySinkOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "mock_hungry_sink", plan_node_id) {}
    ~MockHungrySinkOperatorFactory() override = default;

    bool support_event_scheduler() const override { return true; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MockHungrySinkOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

} // namespace

class IntermediateBlockDriverTest : public ::testing::Test {
public:
    void SetUp() override {
        _query_ctx = std::make_shared<QueryContext>();
        _fragment_ctx = std::make_shared<FragmentContext>();
        _exec_group = std::make_shared<NormalExecutionGroup>();
        _mem_tracker = std::make_shared<MemTracker>(-1, "intermediate_block_driver_test");

        auto runtime_state = std::make_shared<RuntimeState>();
        auto* exec_env = ExecEnv::GetInstance();
        runtime_state->set_exec_env(exec_env);
        runtime_state->set_query_execution_services(&exec_env->query_execution_services());
        runtime_state->_obj_pool = std::make_shared<ObjectPool>();
        // process() top-of-loop runs RETURN_IF_LIMIT_EXCEEDED against the instance mem tracker; the default
        // RuntimeState ctor leaves it null, so give it an unlimited one.
        runtime_state->init_instance_mem_tracker();
        _query_ctx->attach_to_runtime_state(runtime_state.get());
        // process() fetches the query mem tracker unconditionally; wire a real one in (enable_spill is off,
        // so it is otherwise never dereferenced).
        _query_ctx->query_runtime_state().set_query_mem_tracker(_mem_tracker.get());
        runtime_state->set_fragment_ctx(_fragment_ctx.get(), &_fragment_ctx->fragment_runtime_state());
        runtime_state->set_fragment_dict_state(_fragment_ctx->dict_state());
        runtime_state->_profile = std::make_shared<RuntimeProfile>("dummy");
        _fragment_ctx->set_runtime_state(std::move(runtime_state));
        _runtime_state = _fragment_ctx->runtime_state();
        _fragment_ctx->init_event_scheduler();
    }

    // Build a real Pipeline + real PipelineDriver from real factories, fully prepared.
    struct DriverFixture {
        Pipeline pipeline;
        PipelineExecutorMetrics metrics;
        std::unique_ptr<DriverQueue> driver_queue;
        std::unique_ptr<PipelineDriver> driver;
        MockInteriorOperator* interior = nullptr;

        DriverFixture(OpFactories factories, ExecutionGroup* exec_group, FragmentContext* fragment_ctx,
                      QueryContext* query_ctx)
                : pipeline(0, std::move(factories), exec_group) {
            auto operators = pipeline.create_operators(1, 0);
            interior = down_cast<MockInteriorOperator*>(operators[1].get());
            auto* query_runtime_state = query_ctx == nullptr ? nullptr : &query_ctx->query_runtime_state();
            auto* fragment_runtime_state = fragment_ctx == nullptr ? nullptr : &fragment_ctx->fragment_runtime_state();
            driver = std::make_unique<PipelineDriver>(operators, query_runtime_state, fragment_runtime_state,
                                                      pipeline.pipeline_event(), &pipeline, nullptr, 1);
            driver_queue = std::make_unique<QuerySharedDriverQueue>(metrics.get_driver_queue_metrics());
            fragment_ctx->event_scheduler()->attach_queue(driver_queue.get());
            driver->set_observer(fragment_ctx->event_scheduler()->create_driver_observer(driver.get()));
            driver->assign_observer();
        }
    };

    std::unique_ptr<DriverFixture> make_driver(bool wakeable_interior) {
        OpFactories factories;
        factories.emplace_back(std::make_shared<MockOpenSourceOperatorFactory>(0, 1));
        factories.emplace_back(std::make_shared<MockInteriorOperatorFactory>(2, 3, wakeable_interior));
        factories.emplace_back(std::make_shared<MockHungrySinkOperatorFactory>(4, 5));
        auto fx = std::make_unique<DriverFixture>(factories, _exec_group.get(), _fragment_ctx.get(), _query_ctx.get());
        return fx;
    }

protected:
    std::shared_ptr<QueryContext> _query_ctx;
    std::shared_ptr<FragmentContext> _fragment_ctx;
    std::shared_ptr<NormalExecutionGroup> _exec_group;
    std::shared_ptr<MemTracker> _mem_tracker;
    RuntimeState* _runtime_state = nullptr;
};

// (a) A real driver with a wakeable interior whose interior need_input() is closed parks in
// INTERMEDIATE_BLOCK after process(); opening the interior lets check_is_ready()/is_not_blocked() release.
TEST_F(IntermediateBlockDriverTest, wakeable_interior_parks_then_releases) {
    auto fx = make_driver(/*wakeable_interior=*/true);
    ASSERT_OK(fx->driver->prepare(_runtime_state));
    ASSERT_OK(fx->driver->prepare_local_state(_runtime_state));

    // Interior block present: the interior refuses input (build-side load latch not ready). Both edges are
    // open (source has_output, sink need_input), so the only classification left is INTERMEDIATE_BLOCK.
    ASSERT_FALSE(fx->interior->need_input());

    auto state = fx->driver->process(_runtime_state, 0);
    ASSERT_TRUE(state.ok()) << state.status();
    ASSERT_EQ(DriverState::INTERMEDIATE_BLOCK, state.value());
    ASSERT_EQ(DriverState::INTERMEDIATE_BLOCK, fx->driver->driver_state());

    // While still blocked, the schedule-time re-check reports not-ready (would re-park on the next quantum).
    ASSERT_FALSE(fx->driver->check_is_ready());
    auto not_blocked = fx->driver->is_not_blocked();
    ASSERT_TRUE(not_blocked.ok());
    ASSERT_FALSE(not_blocked.value());

    // A notify flips the interior predicate (load latch ready). The interior-pair re-check now reports
    // the driver releasable.
    fx->interior->open_input();
    ASSERT_TRUE(fx->driver->check_is_ready());
    auto released = fx->driver->is_not_blocked();
    ASSERT_TRUE(released.ok());
    ASSERT_TRUE(released.value());
}

// (b) Cancel releases a parked driver: FragmentContext::cancel() -> EventScheduler::try_schedule routes a
// canceled fragment's driver to the ready queue before any block predicate (the released-first
// is_cancelled branch), so a driver parked in INTERMEDIATE_BLOCK is freed immediately by KILL.
TEST_F(IntermediateBlockDriverTest, cancel_releases_parked_driver) {
    auto fx = make_driver(/*wakeable_interior=*/true);
    ASSERT_OK(fx->driver->prepare(_runtime_state));
    ASSERT_OK(fx->driver->prepare_local_state(_runtime_state));

    auto state = fx->driver->process(_runtime_state, 0);
    ASSERT_TRUE(state.ok()) << state.status();
    ASSERT_EQ(DriverState::INTERMEDIATE_BLOCK, fx->driver->driver_state());

    // The driver is parked and the interior is still closed -- without cancel it would stay blocked.
    ASSERT_FALSE(fx->driver->check_is_ready());

    // KILL: cancel the fragment, then run the scheduler's release path. try_schedule's first branch is the
    // released-first is_cancelled() test, so the still-closed interior does not matter -- the driver is put
    // back to the ready queue regardless.
    _fragment_ctx->cancel(Status::Cancelled("killed"));
    ASSERT_TRUE(_runtime_state->is_cancelled());

    fx->driver->set_in_blocked(true);
    _fragment_ctx->event_scheduler()->try_schedule(fx->driver.get());

    // try_schedule moved the canceled driver out of the blocked set onto the ready queue.
    ASSERT_FALSE(fx->driver->is_in_blocked());
    auto picked = fx->driver_queue->take(false);
    ASSERT_TRUE(picked.ok());
    ASSERT_EQ(fx->driver.get(), picked.value());
}

// (c) Control: the same interior block with a non-wakeable interior is not classified INTERMEDIATE_BLOCK.
// Nobody promises a wakeup, so parking would sleep with nobody to wake it; the driver stays READY (and
// keeps polling) instead.
TEST_F(IntermediateBlockDriverTest, non_wakeable_interior_stays_ready) {
    auto fx = make_driver(/*wakeable_interior=*/false);
    ASSERT_OK(fx->driver->prepare(_runtime_state));
    ASSERT_OK(fx->driver->prepare_local_state(_runtime_state));

    // Identical block shape to case (a): interior refuses input, both edges open.
    ASSERT_FALSE(fx->interior->need_input());
    ASSERT_FALSE(fx->interior->supports_intermediate_wakeup());

    auto state = fx->driver->process(_runtime_state, 0);
    ASSERT_TRUE(state.ok()) << state.status();
    ASSERT_NE(DriverState::INTERMEDIATE_BLOCK, state.value());
    ASSERT_EQ(DriverState::READY, state.value());
    ASSERT_EQ(DriverState::READY, fx->driver->driver_state());
}

} // namespace starrocks::pipeline
