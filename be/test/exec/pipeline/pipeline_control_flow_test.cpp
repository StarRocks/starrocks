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

#include <mutex>
#include <random>
#include <utility>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "common/config_exec_flow_fwd.h"
#include "common/util/thrift_util.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/global_spill_manager.h"
#include "exec/spill/operator_mem_resource_manager.h"
#include "pipeline_test_base.h"
#include "runtime/service_contexts.h"

#define ASSERT_COUNTER_LIFETIME(counter, dop)       \
    do {                                            \
        ASSERT_EQ(dop, counter->prepare_times());   \
        ASSERT_EQ(dop, counter->finishing_times()); \
        ASSERT_EQ(dop, counter->finished_times());  \
        ASSERT_EQ(dop, counter->closed_times());    \
        ASSERT_EQ(0, counter->wrong_times());       \
    } while (false)

#define ASSERT_COUNTER_CHUNK_NUM(counter, dop, expected_push_chunk_num, expected_pull_chunk_num) \
    do {                                                                                         \
        ASSERT_COUNTER_LIFETIME(counter, dop);                                                   \
        ASSERT_EQ(expected_push_chunk_num, counter->push_chunk_num());                           \
        ASSERT_EQ(expected_pull_chunk_num, counter->pull_chunk_num());                           \
    } while (false)

#define ASSERT_COUNTER_CHUNK_ROW_NUM(counter, dop, expected_push_chunk_row_num, expected_pull_chunk_row_num) \
    do {                                                                                                     \
        ASSERT_COUNTER_LIFETIME(counter, dop);                                                               \
        ASSERT_EQ(expected_push_chunk_row_num, counter->push_chunk_row_num());                               \
        ASSERT_EQ(expected_pull_chunk_row_num, counter->pull_chunk_row_num());                               \
    } while (false)

namespace starrocks::pipeline {

class Counter {
public:
    void process_prepare() {
        std::lock_guard<std::mutex> l(_mutex);
        _prepare_times++;
    }

    void process_push(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_push_chunk_num;
        _push_chunk_row_num += chunk->num_rows();
    }

    void process_pull(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_pull_chunk_num;
        _pull_chunk_row_num += chunk->num_rows();
    }

    void process_finishing() {
        std::lock_guard<std::mutex> l(_mutex);
        _finishing_times++;
    }

    void process_finished() {
        std::lock_guard<std::mutex> l(_mutex);
        _finished_times++;
    }

    void process_closed() {
        std::lock_guard<std::mutex> l(_mutex);
        _closed_times++;
    }

    void process_wrong() {
        std::lock_guard<std::mutex> l(_mutex);
        _wrong_state_times++;
    }

    size_t prepare_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _prepare_times;
    }

    size_t finishing_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _finishing_times;
    }

    size_t finished_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _finished_times;
    }

    size_t closed_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _closed_times;
    }

    size_t push_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_num;
    }

    size_t pull_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_num;
    }

    size_t push_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_row_num;
    }

    size_t pull_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_row_num;
    }

    size_t wrong_times() {
        std::lock_guard<std::mutex> l(_mutex);
        return _wrong_state_times;
    }

private:
    std::mutex _mutex;
    size_t _push_chunk_num = 0;
    size_t _pull_chunk_num = 0;
    size_t _push_chunk_row_num = 0;
    size_t _pull_chunk_row_num = 0;
    size_t _prepare_times = 0;
    size_t _finishing_times = 0;
    size_t _finished_times = 0;
    size_t _closed_times = 0;
    size_t _wrong_state_times = 0;
};

using CounterPtr = std::shared_ptr<Counter>;

class TestOperator : public Operator {
public:
    TestOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                 int32_t driver_sequence, CounterPtr counter)
            : Operator(factory, id, name, plan_node_id, false, driver_sequence), _counter(std::move(counter)) {}
    ~TestOperator() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Operator::prepare(state));
        _counter->process_prepare();
        return Status::OK();
    }

    Status set_finishing(RuntimeState* state) override {
        _counter->process_finishing();
        return Status::OK();
    }

    Status set_finished(RuntimeState* state) override {
        if (!is_finished()) {
            _counter->process_wrong();
        }
        _counter->process_finished();
        return Status::OK();
    }

    void close(RuntimeState* state) override {
        _counter->process_closed();
        return Operator::close(state);
    }

protected:
    CounterPtr _counter = nullptr;
};

class TestSourceOperator : public SourceOperator {
public:
    TestSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       size_t chunk_num, size_t chunk_size, CounterPtr counter, int32_t pending_finish_cnt)
            : SourceOperator(factory, id, "test_source", plan_node_id, false, driver_sequence),
              _counter(std::move(counter)),
              _pending_finish_cnt(pending_finish_cnt) {
        for (size_t i = 0; i < chunk_num; ++i) {
            _chunks.push_back(PipelineTestBase::_create_and_fill_chunk(chunk_size));
        }
    }
    ~TestSourceOperator() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(SourceOperator::prepare(state));
        _counter->process_prepare();
        return Status::OK();
    }

    Status set_finishing(RuntimeState* state) override {
        _counter->process_finishing();
        return Status::OK();
    }

    Status set_finished(RuntimeState* state) override {
        _counter->process_finished();
        return Status::OK();
    }

    void close(RuntimeState* state) override {
        ASSERT_LT(_pending_finish_cnt, 0);
        _counter->process_closed();
        return SourceOperator::close(state);
    }

    bool has_output() const override { return _index < _chunks.size(); }
    bool is_finished() const override { return !has_output(); }
    bool pending_finish() const override { return --_pending_finish_cnt >= 0; }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    CounterPtr _counter;
    std::vector<ChunkPtr> _chunks;
    size_t _index = 0;
    mutable std::atomic<int32_t> _pending_finish_cnt;
};

Status TestSourceOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _counter->process_push(chunk);
    return Status::InternalError("Shouldn't push chunk to source operator");
}

StatusOr<ChunkPtr> TestSourceOperator::pull_chunk(RuntimeState* state) {
    auto chunk = _chunks[_index++];
    _counter->process_pull(chunk);
    return chunk;
}

class TestSourceOperatorFactory final : public SourceOperatorFactory {
public:
    TestSourceOperatorFactory(int32_t id, int32_t plan_node_id, size_t chunk_num, size_t chunk_size, CounterPtr counter,
                              int32_t pending_finish_cnt)
            : SourceOperatorFactory(id, "test_source", plan_node_id),
              _chunk_num(chunk_num),
              _chunk_size(chunk_size),
              _counter(std::move(counter)),
              _pending_finish_cnt(pending_finish_cnt) {}

    ~TestSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSourceOperator>(this, _id, _plan_node_id, driver_sequence, _chunk_num, _chunk_size,
                                                    _counter, _pending_finish_cnt);
    }
    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

private:
    size_t _chunk_num;
    size_t _chunk_size;
    CounterPtr _counter;
    int32_t _pending_finish_cnt;
};

class TestNormalOperator : public TestOperator {
public:
    TestNormalOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                       CounterPtr counter)
            : TestOperator(factory, id, "test_normal", plan_node_id, driver_sequence, std::move(counter)) {}
    ~TestNormalOperator() override = default;

    bool need_input() const override { return true; }
    bool has_output() const override { return _chunk != nullptr; }
    bool is_finished() const override { return _is_finished && !has_output(); }
    Status set_finishing(RuntimeState* state) override {
        CHECK(TestOperator::set_finishing(state).ok());
        _is_finished = true;
        return Status::OK();
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    bool _is_finished = false;
    ChunkPtr _chunk;
};

Status TestNormalOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _chunk = chunk;
    _counter->process_push(chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> TestNormalOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk = _chunk;
    _counter->process_pull(chunk);
    _chunk = nullptr;
    return chunk;
}

class TestNormalOperatorFactory final : public OperatorFactory {
public:
    TestNormalOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : OperatorFactory(id, "test_normal", plan_node_id), _counter(std::move(counter)) {}

    ~TestNormalOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestNormalOperator>(this, _id, _plan_node_id, driver_sequence, _counter);
    }

private:
    CounterPtr _counter;
};

class TestSinkOperator : public TestOperator {
public:
    TestSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                     CounterPtr counter)
            : TestOperator(factory, id, "test_sink", plan_node_id, driver_sequence, std::move(counter)) {}
    ~TestSinkOperator() override = default;

    bool need_input() const override { return true; }
    bool has_output() const override { return false; }
    bool is_finished() const override { return _is_finished; }
    Status set_finishing(RuntimeState* state) override {
        CHECK(TestOperator::set_finishing(state).ok());
        _is_finished = true;
        return Status::OK();
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    bool _is_finished = false;
};

Status TestSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _counter->process_push(chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> TestSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk to sink operator");
}

class TestSinkOperatorFactory final : public OperatorFactory {
public:
    TestSinkOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : OperatorFactory(id, "test_sink", plan_node_id), _counter(std::move(counter)) {}

    ~TestSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestSinkOperator>(this, _id, _plan_node_id, driver_sequence, _counter);
    }

private:
    CounterPtr _counter;
};

class RuntimeAccessProbeOperator final : public Operator {
public:
    RuntimeAccessProbeOperator(OperatorFactory* factory, int32_t driver_sequence)
            : Operator(factory, factory->id(), factory->get_raw_name(), factory->plan_node_id(), false,
                       driver_sequence) {}

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

class RuntimeAccessProbeFactory final : public OperatorFactory {
public:
    RuntimeAccessProbeFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "runtime_access_probe", plan_node_id) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<RuntimeAccessProbeOperator>(this, driver_sequence);
    }

    void bind_runtime_in_filters(RuntimeState* state, int32_t driver_sequence,
                                 std::vector<ExprContext*>* runtime_in_filters) override {
        ++bind_calls;
        last_state = state;
        last_driver_sequence = driver_sequence;
        runtime_in_filters->insert(runtime_in_filters->end(), local_filters.begin(), local_filters.end());
        runtime_in_filters->insert(runtime_in_filters->end(), instance_filters.begin(), instance_filters.end());
    }

    int bind_calls = 0;
    RuntimeState* last_state = nullptr;
    int32_t last_driver_sequence = -1;
    std::vector<ExprContext*> local_filters;
    std::vector<ExprContext*> instance_filters;
};

class TestPipelineControlFlow : public PipelineTestBase {};

class SpillLifecycleSourceOperator final : public SourceOperator {
public:
    SpillLifecycleSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                 bool spillable, bool releaseable, Status prepare_status = Status::OK())
            : SourceOperator(factory, id, "spill_lifecycle_source", plan_node_id, false, driver_sequence),
              _spillable(spillable),
              _releaseable(releaseable),
              _prepare_status(std::move(prepare_status)) {}

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(SourceOperator::prepare(state));
        return _prepare_status;
    }

    bool has_output() const override { return false; }
    bool need_input() const override { return false; }
    bool is_finished() const override { return true; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }

    bool spillable() const override { return _spillable; }
    bool releaseable() const override { return _releaseable; }

private:
    bool _spillable;
    bool _releaseable;
    Status _prepare_status;
};

class SpillLifecycleSourceOperatorFactory final : public SourceOperatorFactory {
public:
    SpillLifecycleSourceOperatorFactory(int32_t id, int32_t plan_node_id, bool spillable, bool releaseable,
                                        Status prepare_status = Status::OK())
            : SourceOperatorFactory(id, "spill_lifecycle_source", plan_node_id),
              _spillable(spillable),
              _releaseable(releaseable),
              _prepare_status(std::move(prepare_status)) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SpillLifecycleSourceOperator>(this, _id, _plan_node_id, driver_sequence, _spillable,
                                                              _releaseable, _prepare_status);
    }

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

private:
    bool _spillable;
    bool _releaseable;
    Status _prepare_status;
};

class SpillLifecycleOperator final : public Operator {
public:
    SpillLifecycleOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                           bool spillable, bool releaseable, Status prepare_status = Status::OK())
            : Operator(factory, id, "spill_lifecycle_operator", plan_node_id, false, driver_sequence),
              _spillable(spillable),
              _releaseable(releaseable),
              _prepare_status(std::move(prepare_status)) {}

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Operator::prepare(state));
        return _prepare_status;
    }

    bool has_output() const override { return false; }
    bool need_input() const override { return false; }
    bool is_finished() const override { return true; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }

    bool spillable() const override { return _spillable; }
    bool releaseable() const override { return _releaseable; }

private:
    bool _spillable;
    bool _releaseable;
    Status _prepare_status;
};

class SpillLifecycleOperatorFactory final : public OperatorFactory {
public:
    SpillLifecycleOperatorFactory(int32_t id, int32_t plan_node_id, bool spillable, bool releaseable,
                                  Status prepare_status = Status::OK())
            : OperatorFactory(id, "spill_lifecycle_operator", plan_node_id),
              _spillable(spillable),
              _releaseable(releaseable),
              _prepare_status(std::move(prepare_status)) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SpillLifecycleOperator>(this, _id, _plan_node_id, driver_sequence, _spillable,
                                                        _releaseable, _prepare_status);
    }

private:
    bool _spillable;
    bool _releaseable;
    Status _prepare_status;
};

class TestPipelineDriver final : public PipelineDriver {
public:
    using PipelineDriver::PipelineDriver;

    void close_operators_for_test(RuntimeState* runtime_state) { _close_operators(runtime_state); }
};

struct SpillDriverTestHarness {
    spill::GlobalSpillManager global_spill_manager;
    spill::DirManager spill_dir_manager;
    RuntimeServices runtime_services;
    QueryExecutionServices query_execution_services;
    QueryContext query_ctx;
    FragmentContext fragment_ctx;
    Pipeline pipeline;

    SpillDriverTestHarness() : pipeline(1, {}, nullptr) {
        runtime_services.spill_dir_mgr = &spill_dir_manager;
        runtime_services.global_spill_manager = &global_spill_manager;
        query_execution_services.runtime = &runtime_services;
        query_ctx.set_query_id(generate_uuid());
        query_ctx.set_query_execution_services(&query_execution_services);
        EXPECT_OK(query_ctx.init_spill_manager(TQueryOptions{}));
        query_ctx.set_query_execution_services(nullptr);

        fragment_ctx.set_query_id(query_ctx.query_id());
        fragment_ctx.set_fragment_instance_id(generate_uuid());
        auto runtime_state = std::make_shared<RuntimeState>(TQueryGlobals{});
        runtime_state->set_query_ctx(&query_ctx);
        runtime_state->set_fragment_ctx(&fragment_ctx);
        fragment_ctx.set_runtime_state(std::move(runtime_state));
    }

    RuntimeState* state() const { return fragment_ctx.runtime_state(); }
};

TEST(OperatorRuntimeAccessTest, test_operator_precondition_ready_uses_runtime_access) {
    RuntimeAccessProbeFactory factory(1, 2);
    int local_filter_sentinel = 0;
    int instance_filter_a_sentinel = 0;
    int instance_filter_b_sentinel = 0;
    factory.local_filters = {reinterpret_cast<ExprContext*>(&local_filter_sentinel)};
    factory.instance_filters = {reinterpret_cast<ExprContext*>(&instance_filter_a_sentinel),
                                reinterpret_cast<ExprContext*>(&instance_filter_b_sentinel)};

    RuntimeState state;
    auto op = factory.create(1, 7);

    op->set_precondition_ready(&state);

    ASSERT_EQ(1, factory.bind_calls);
    ASSERT_EQ(&state, factory.last_state);
    ASSERT_EQ(7, factory.last_driver_sequence);
    ASSERT_EQ(3, op->runtime_in_filters().size());
    EXPECT_EQ(factory.local_filters[0], op->runtime_in_filters()[0]);
    EXPECT_EQ(factory.instance_filters[0], op->runtime_in_filters()[1]);
    EXPECT_EQ(factory.instance_filters[1], op->runtime_in_filters()[2]);
}

TEST(OperatorExecStatsSnapshotTest, test_default_operator_snapshot) {
    TestNormalOperatorFactory factory(1, 10, std::make_shared<Counter>());
    RuntimeState state;
    auto op = factory.create(1, 0);

    ASSERT_OK(op->prepare_local_state(&state));
    COUNTER_UPDATE(op->_push_row_num_counter, 5);
    COUNTER_UPDATE(op->_pull_row_num_counter, 3);
    op->_init_conjuct_counters();
    COUNTER_UPDATE(op->_conjuncts_input_counter, 11);
    COUNTER_UPDATE(op->_conjuncts_output_counter, 7);
    op->_init_rf_counters(true);
    COUNTER_UPDATE(op->_bloom_filter_eval_context.join_runtime_filter_input_counter, 13);
    COUNTER_UPDATE(op->_bloom_filter_eval_context.join_runtime_filter_output_counter, 9);

    auto snapshot = op->exec_stats_snapshot();

    EXPECT_TRUE(snapshot.valid);
    EXPECT_TRUE(snapshot.require_registered_plan_node);
    EXPECT_EQ(10, snapshot.plan_node_id);
    EXPECT_TRUE(snapshot.update_push_rows);
    EXPECT_TRUE(snapshot.update_pull_rows);
    EXPECT_FALSE(snapshot.force_set_pull_rows);
    EXPECT_EQ(5, snapshot.push_rows);
    EXPECT_EQ(3, snapshot.pull_rows);
    EXPECT_TRUE(snapshot.update_pred_filter_rows);
    EXPECT_EQ(4, snapshot.pred_filter_rows);
    EXPECT_TRUE(snapshot.update_rf_filter_rows);
    EXPECT_EQ(4, snapshot.rf_filter_rows);
}

TEST(OperatorExecStatsSnapshotTest, test_limit_snapshot_force_sets_pull_rows) {
    LimitOperatorFactory factory(1, 20, 100);
    RuntimeState state;
    auto op = factory.create(1, 0);

    ASSERT_OK(op->prepare_local_state(&state));
    COUNTER_UPDATE(op->_pull_row_num_counter, 6);
    op->_init_conjuct_counters();
    COUNTER_UPDATE(op->_conjuncts_input_counter, 10);
    COUNTER_UPDATE(op->_conjuncts_output_counter, 4);
    op->_init_rf_counters(true);
    COUNTER_UPDATE(op->_bloom_filter_eval_context.join_runtime_filter_input_counter, 8);
    COUNTER_UPDATE(op->_bloom_filter_eval_context.join_runtime_filter_output_counter, 3);

    auto snapshot = op->exec_stats_snapshot();

    EXPECT_TRUE(snapshot.valid);
    EXPECT_FALSE(snapshot.require_registered_plan_node);
    EXPECT_EQ(20, snapshot.plan_node_id);
    EXPECT_FALSE(snapshot.update_push_rows);
    EXPECT_TRUE(snapshot.update_pull_rows);
    EXPECT_TRUE(snapshot.force_set_pull_rows);
    EXPECT_EQ(6, snapshot.pull_rows);
    EXPECT_TRUE(snapshot.update_pred_filter_rows);
    EXPECT_EQ(6, snapshot.pred_filter_rows);
    EXPECT_TRUE(snapshot.update_rf_filter_rows);
    EXPECT_EQ(5, snapshot.rf_filter_rows);
}

TEST(PipelineDriverSpillResourceManagerTest, test_operator_manager_lifecycle) {
    SpillDriverTestHarness harness;
    SpillLifecycleSourceOperatorFactory source_factory(1, 10, false, true);
    SpillLifecycleOperatorFactory spillable_factory(2, 20, true, false);
    Operators operators{source_factory.create(1, 0), spillable_factory.create(1, 0)};

    TestPipelineDriver driver(operators, &harness.query_ctx, &harness.fragment_ctx, &harness.pipeline, -1);

    ASSERT_OK(driver.prepare(harness.state()));
    ASSERT_EQ(config::local_exchange_buffer_mem_limit_per_driver +
                      spill::OperatorMemoryResourceManager::compute_available_memory_bytes(*harness.state()),
              harness.global_spill_manager.spill_expected_reserved_bytes());
    ASSERT_EQ(1, harness.global_spill_manager.spillable_operators());

    ASSERT_OK(driver.prepare_local_state(harness.state()));
    driver.close_operators_for_test(harness.state());
    EXPECT_EQ(0, harness.global_spill_manager.spill_expected_reserved_bytes());
    EXPECT_EQ(0, harness.global_spill_manager.spillable_operators());
}

TEST(PipelineDriverSpillResourceManagerTest, test_prepare_failure_rolls_back_all_prepared_managers) {
    SpillDriverTestHarness harness;
    SpillLifecycleSourceOperatorFactory source_factory(1, 10, false, true);
    SpillLifecycleOperatorFactory failing_factory(2, 20, true, false, Status::InternalError("prepare failed"));
    Operators operators{source_factory.create(1, 0), failing_factory.create(1, 0)};

    TestPipelineDriver driver(operators, &harness.query_ctx, &harness.fragment_ctx, &harness.pipeline, -1);

    auto st = driver.prepare(harness.state());
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error());
    EXPECT_EQ(0, harness.global_spill_manager.spill_expected_reserved_bytes());
    EXPECT_EQ(0, harness.global_spill_manager.spillable_operators());
}

TEST_F(TestPipelineControlFlow, test_two_operatories) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1;
    _vector_chunk_size = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories, exec_group.get()));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}

TEST_F(TestPipelineControlFlow, test_three_operatories) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1;
    _vector_chunk_size = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr normalCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestNormalOperatorFactory>(next_operator_id(), next_plan_node_id(), normalCounter));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories, exec_group.get()));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(normalCounter, 1, chunk_num, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}

TEST_F(TestPipelineControlFlow, test_multi_operators) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t max_mid_operator_num = 128;
    size_t chunk_num = 1;
    _vector_chunk_size = 1;

    for (size_t i = 1; i <= max_mid_operator_num; ++i) {
        CounterPtr sourceCounter = std::make_shared<Counter>();
        CounterPtr sinkCounter = std::make_shared<Counter>();
        std::vector<CounterPtr> normalCounters;
        for (size_t j = 0; j < i; ++j) {
            normalCounters.push_back(std::make_shared<Counter>());
        }

        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories;

            op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                    next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
            for (size_t j = 0; j < i; ++j) {
                op_factories.push_back(std::make_shared<TestNormalOperatorFactory>(
                        next_operator_id(), next_plan_node_id(), normalCounters[j]));
            }
            op_factories.push_back(
                    std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

            _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories, exec_group.get()));
        };

        start_test();

        ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
        ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
        for (size_t j = 0; j < i; ++j) {
            ASSERT_COUNTER_CHUNK_NUM(normalCounters[j], 1, chunk_num, chunk_num);
        }
        ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
    }
}

TEST_F(TestPipelineControlFlow, test_full_chunk_size) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr normalCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestNormalOperatorFactory>(next_operator_id(), next_plan_node_id(), normalCounter));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories, exec_group.get()));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(normalCounter, 1, chunk_num, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}

TEST_F(TestPipelineControlFlow, test_multi_chunks) {
    std::default_random_engine e;
    std::uniform_int_distribution<int32_t> u32(0, 100);
    size_t chunk_num = 1000;
    _vector_chunk_size = 1;
    CounterPtr sourceCounter = std::make_shared<Counter>();
    CounterPtr normalCounter = std::make_shared<Counter>();
    CounterPtr sinkCounter = std::make_shared<Counter>();

    _pipeline_builder = [&](RuntimeState* state) {
        OpFactories op_factories;

        op_factories.push_back(std::make_shared<TestSourceOperatorFactory>(
                next_operator_id(), next_plan_node_id(), chunk_num, _vector_chunk_size, sourceCounter, u32(e)));
        op_factories.push_back(
                std::make_shared<TestNormalOperatorFactory>(next_operator_id(), next_plan_node_id(), normalCounter));
        op_factories.push_back(
                std::make_shared<TestSinkOperatorFactory>(next_operator_id(), next_plan_node_id(), sinkCounter));

        _pipelines.push_back(std::make_shared<Pipeline>(next_pipeline_id(), op_factories, exec_group.get()));
    };

    start_test();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));
    ASSERT_COUNTER_CHUNK_NUM(sourceCounter, 1, 0, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(normalCounter, 1, chunk_num, chunk_num);
    ASSERT_COUNTER_CHUNK_NUM(sinkCounter, 1, chunk_num, 0);
}
} // namespace starrocks::pipeline
