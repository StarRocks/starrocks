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

// Query cache wraps the pre-cache operators in MultilaneOperator (and conjugates a pre-cache sink/source
// pair in ConjugateOperator), so PipelineDriver::_operators and the factory chain hold the wrapper, not
// the wrapped operator. The driver-side aggregations (is_still_pending_finish, the wakeable-intermediate
// registry, the park-time BlockReason check) and the fragment gate
// (Pipeline::all_support_event_scheduler) therefore only see what the wrapper forwards. These tests pin
// that forwarding: a cache-wrapped spill operator must keep its driver in PENDING_FINISH while spill IO
// is in flight, and must stay visible to the event-scheduler gate and the wakeup machinery.

#include <gtest/gtest.h>

#include <atomic>
#include <memory>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_factory.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exec/query_cache/multilane_operator.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exec/runtime/pipeline.h"
#include "exec/runtime/pipeline_driver.h"
#include "exec/runtime/schedule/event_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks::query_cache {

using namespace starrocks::pipeline;

namespace {

// A spill-like operator with controllable pending IO and the full wakeup surface, standing in for a
// spilled probe / agg sink behind the cache wrapper.
class MockSpillLikeOperator final : public Operator {
public:
    MockSpillLikeOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "mock_spill_like", plan_node_id, false, driver_sequence) {}
    ~MockSpillLikeOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return false; }

    bool pending_finish() const override { return _pending_io.load(); }
    bool supports_intermediate_wakeup() const override { return true; }
    BlockReason block_reason() const override { return BlockReason::WAIT_RESTORE; }
    uint32_t covered_wakeups() const override { return block_reason_bit(BlockReason::WAIT_RESTORE); }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }

    void finish_io() { _pending_io.store(false); }

private:
    std::atomic<bool> _pending_io{true};
};

class MockSpillLikeOperatorFactory final : public OperatorFactory {
public:
    MockSpillLikeOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "mock_spill_like", plan_node_id) {}
    ~MockSpillLikeOperatorFactory() override = default;

    bool support_event_scheduler() const override { return true; }
    bool supports_intermediate_wakeup() const override { return true; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MockSpillLikeOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

// An inert operator with all defaults (no pending IO, no wakeup), for the conjugate's other half.
class MockInertOperator final : public Operator {
public:
    MockInertOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "mock_inert", plan_node_id, false, driver_sequence) {}
    ~MockInertOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return false; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

class MockInertOperatorFactory final : public OperatorFactory {
public:
    MockInertOperatorFactory(int32_t id, int32_t plan_node_id) : OperatorFactory(id, "mock_inert", plan_node_id) {}
    ~MockInertOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<MockInertOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

// Driver-fixture edges, as in intermediate_block_driver_test: an always-open source and an always-hungry
// sink keep both edges off the INPUT_EMPTY / OUTPUT_FULL classifications.
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

// MultilaneOperator forwards the four driver-side aggregation methods to its lanes.
TEST(CacheWrapperForwardingTest, multilane_operator_forwards) {
    auto wrapped_factory = std::make_shared<MockSpillLikeOperatorFactory>(1, 2);
    MultilaneOperatorFactory ml_factory(3, wrapped_factory, /*num_lanes=*/2);
    auto op = ml_factory.create(1, 0);
    auto* ml_op = down_cast<MultilaneOperator*>(op.get());

    ASSERT_TRUE(ml_op->pending_finish());
    ASSERT_TRUE(ml_op->supports_intermediate_wakeup());
    ASSERT_EQ(BlockReason::WAIT_RESTORE, ml_op->block_reason());
    ASSERT_EQ(block_reason_bit(BlockReason::WAIT_RESTORE), ml_op->covered_wakeups());

    // pending_finish is any-of: it holds while any lane still has IO in flight and drops when the last
    // lane drains.
    down_cast<MockSpillLikeOperator*>(ml_op->get_internal_op(0).get())->finish_io();
    ASSERT_TRUE(ml_op->pending_finish());
    down_cast<MockSpillLikeOperator*>(ml_op->get_internal_op(1).get())->finish_io();
    ASSERT_FALSE(ml_op->pending_finish());
}

// MultilaneOperatorFactory forwards the fragment-gate methods to the wrapped factory.
TEST(CacheWrapperForwardingTest, multilane_factory_forwards_gate) {
    auto wakeable = std::make_shared<MockSpillLikeOperatorFactory>(1, 2);
    MultilaneOperatorFactory ml_wakeable(3, wakeable, 1);
    ASSERT_TRUE(ml_wakeable.support_event_scheduler());
    ASSERT_TRUE(ml_wakeable.supports_intermediate_wakeup());

    auto inert = std::make_shared<MockInertOperatorFactory>(4, 5);
    MultilaneOperatorFactory ml_inert(6, inert, 1);
    ASSERT_FALSE(ml_inert.support_event_scheduler());
    ASSERT_FALSE(ml_inert.supports_intermediate_wakeup());
}

// ConjugateOperator combines the two halves: pending IO or a wakeup on either half is visible on the
// wrapper, and the sink half names its reason first.
TEST(CacheWrapperForwardingTest, conjugate_operator_forwards) {
    auto sink_factory = std::make_shared<MockSpillLikeOperatorFactory>(1, 2);
    auto source_factory = std::make_shared<MockInertOperatorFactory>(3, 4);
    ConjugateOperatorFactory conj_factory(sink_factory, source_factory);
    auto op = conj_factory.create(1, 0);
    auto* conj_op = down_cast<ConjugateOperator*>(op.get());

    ASSERT_TRUE(conj_op->pending_finish());
    ASSERT_TRUE(conj_op->supports_intermediate_wakeup());
    ASSERT_EQ(BlockReason::WAIT_RESTORE, conj_op->block_reason());
    ASSERT_EQ(block_reason_bit(BlockReason::WAIT_RESTORE), conj_op->covered_wakeups());

    ASSERT_TRUE(conj_factory.support_event_scheduler() ==
                (sink_factory->support_event_scheduler() && source_factory->support_event_scheduler()));
    ASSERT_TRUE(conj_factory.supports_intermediate_wakeup());
}

// Driver-level, the cache shape itself: a real PipelineDriver whose interior operator is a
// MultilaneOperator-wrapped spill-like operator. is_still_pending_finish() is the exact aggregate that
// decides PENDING_FINISH vs FINISH in process(), check_short_circuit() and the cancel path; it must hold
// the driver while the wrapped operator has IO in flight and release it when the IO drains.
class CacheWrapperDriverTest : public ::testing::Test {
public:
    void SetUp() override {
        _query_ctx = std::make_shared<QueryContext>();
        _fragment_ctx = std::make_shared<FragmentContext>();
        _exec_group = std::make_shared<NormalExecutionGroup>();
        _mem_tracker = std::make_shared<MemTracker>(-1, "cache_wrapper_forwarding_test");

        auto runtime_state = std::make_shared<RuntimeState>();
        auto* exec_env = ExecEnv::GetInstance();
        runtime_state->set_exec_env(exec_env);
        runtime_state->set_query_execution_services(&exec_env->query_execution_services());
        runtime_state->_obj_pool = std::make_shared<ObjectPool>();
        runtime_state->init_instance_mem_tracker();
        _query_ctx->attach_to_runtime_state(runtime_state.get());
        _query_ctx->query_runtime_state().set_query_mem_tracker(_mem_tracker.get());
        runtime_state->set_fragment_ctx(_fragment_ctx.get(), &_fragment_ctx->fragment_runtime_state());
        runtime_state->set_fragment_dict_state(_fragment_ctx->dict_state());
        runtime_state->_profile = std::make_shared<RuntimeProfile>("dummy");
        _fragment_ctx->set_runtime_state(std::move(runtime_state));
        _runtime_state = _fragment_ctx->runtime_state();
        _fragment_ctx->init_event_scheduler();
    }

protected:
    std::shared_ptr<QueryContext> _query_ctx;
    std::shared_ptr<FragmentContext> _fragment_ctx;
    std::shared_ptr<NormalExecutionGroup> _exec_group;
    std::shared_ptr<MemTracker> _mem_tracker;
    RuntimeState* _runtime_state = nullptr;
};

TEST_F(CacheWrapperDriverTest, wrapped_pending_finish_holds_driver) {
    OpFactories factories;
    factories.emplace_back(std::make_shared<MockOpenSourceOperatorFactory>(0, 1));
    auto wrapped_factory = std::make_shared<MockSpillLikeOperatorFactory>(2, 3);
    factories.emplace_back(std::make_shared<MultilaneOperatorFactory>(4, wrapped_factory, /*num_lanes=*/1));
    factories.emplace_back(std::make_shared<MockHungrySinkOperatorFactory>(5, 6));

    Pipeline pipeline(0, std::move(factories), _exec_group.get());
    auto operators = pipeline.create_operators(1, 0);
    auto* ml_op = down_cast<MultilaneOperator*>(operators[1].get());
    auto driver = std::make_unique<PipelineDriver>(operators, &_query_ctx->query_runtime_state(),
                                                   &_fragment_ctx->fragment_runtime_state(), pipeline.pipeline_event(),
                                                   &pipeline, nullptr, 1);
    driver->set_observer(_fragment_ctx->event_scheduler()->create_driver_observer(driver.get()));
    driver->assign_observer();
    ASSERT_OK(driver->prepare(_runtime_state));

    // The wrapped operator still has spill IO in flight: the driver must report pending finish, so
    // process()/check_short_circuit()/the cancel path keep it in PENDING_FINISH instead of tearing the
    // wrapped operator down under its IO tasks.
    ASSERT_TRUE(driver->is_still_pending_finish());

    // The IO drains; the driver is releasable.
    down_cast<MockSpillLikeOperator*>(ml_op->get_internal_op(0).get())->finish_io();
    ASSERT_FALSE(driver->is_still_pending_finish());
}

// The fragment gate sees the wrapped chain: every factory in [source, multilane(spill-like), sink]
// reports event-scheduler support through the wrapper, so the cached fragment stays eligible for the
// event scheduler exactly when its unwrapped twin is.
TEST_F(CacheWrapperDriverTest, gate_sees_through_wrapper) {
    OpFactories factories;
    factories.emplace_back(std::make_shared<MockOpenSourceOperatorFactory>(0, 1));
    auto wrapped_factory = std::make_shared<MockSpillLikeOperatorFactory>(2, 3);
    factories.emplace_back(std::make_shared<MultilaneOperatorFactory>(4, wrapped_factory, /*num_lanes=*/1));
    factories.emplace_back(std::make_shared<MockHungrySinkOperatorFactory>(5, 6));

    Pipeline pipeline(0, std::move(factories), _exec_group.get());
    ASSERT_TRUE(pipeline.all_support_event_scheduler());
}

} // namespace starrocks::query_cache
