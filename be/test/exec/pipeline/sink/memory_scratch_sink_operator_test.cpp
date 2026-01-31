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

#include "exec/pipeline/sink/memory_scratch_sink_operator.h"

#include "exec/pipeline/group_execution/execution_group_builder.h"
#include "gen_cpp/RuntimeProfile_types.h"
#include "gtest/gtest.h"
#include "testutil/assert.h"
#include "util/await.h"

namespace starrocks::pipeline {

namespace {
class MockEmptyOperator final : public SourceOperator {
public:
    MockEmptyOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "mock_empty_operator", plan_node_id, false, driver_sequence) {}

    ~MockEmptyOperator() override = default;

    bool has_output() const override { return true; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return true; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

class MockEmptyOperatorFactory : public SourceOperatorFactory {
public:
    MockEmptyOperatorFactory(int32_t id, std::string name, int32_t plan_node_id)
            : SourceOperatorFactory(id, name, plan_node_id) {}

    virtual OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) {
        return std::make_shared<MockEmptyOperator>(this, _id, _plan_node_id, 1);
    }
};

} // namespace

TEST(MemoryScratchSinkOperatorTest, test_cancel) {
    TExecPlanFragmentParams _request;
    _request.params.fragment_instance_id = generate_uuid();
    _request.params.query_id = generate_uuid();

    const auto& params = _request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    pipeline::QueryContext* _query_ctx;
    pipeline::FragmentContext* _fragment_ctx;
    ExecEnv* _exec_env = ExecEnv::GetInstance();

    ASSIGN_OR_ASSERT_FAIL(_query_ctx, _exec_env->query_context_mgr()->get_or_register(query_id));
    _query_ctx->set_query_id(query_id);
    _query_ctx->set_total_fragments(1);
    _query_ctx->set_delivery_expire_seconds(60);
    _query_ctx->set_query_expire_seconds(60);
    _query_ctx->extend_delivery_lifetime();
    _query_ctx->extend_query_lifetime();
    _query_ctx->set_final_sink();
    _query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                 GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_is_stream_pipeline(true);
    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                           _request.query_options, _request.query_globals, _exec_env));
    RuntimeState* _runtime_state = _fragment_ctx->runtime_state();
    _runtime_state->set_query_ctx(_query_ctx);
    _runtime_state->set_fragment_ctx(_fragment_ctx);

    std::vector<TExpr> t_output_expr;
    RowDescriptor row_desc;

    MockEmptyOperatorFactory factory1(1, "mock_op_factory", 2);
    EXPECT_TRUE(factory1.prepare(_runtime_state).ok());
    MemoryScratchSinkOperatorFactory factory2(2, row_desc, t_output_expr, _fragment_ctx);
    EXPECT_TRUE(factory2.prepare(_runtime_state).ok());

    Status result_st;
    std::shared_ptr<arrow::RecordBatch> record_batch;
    bool eos;
    std::thread t([&] { result_st = _exec_env->result_queue_mgr()->fetch_result(fragment_id, &record_batch, &eos); });

    Operators ops = {factory1.create(1, 1), factory2.create(1, 2)};
    auto exec_group = ExecutionGroupBuilder::create_normal_exec_group();
    auto pipeline = std::make_shared<Pipeline>(0, OpFactories(), exec_group.get());
    auto driver = std::make_shared<PipelineDriver>(ops, _query_ctx, _fragment_ctx, pipeline.get(), 0);

    driver->prepare(_runtime_state);
    driver->prepare_local_state(_runtime_state);

    // Now simulate GlobalDriverExecutor to cancel the driver due to source operator failure
    auto status = Status::NotFound("file not found");
    _query_ctx->cancel(status, false);
    _runtime_state->set_is_cancelled(true);
    driver->cancel_operators(_runtime_state);

    // fetch_result() can be terminated properly after the operator get cancelled.
    t.join();
    EXPECT_FALSE(result_st.ok()) << result_st;
    EXPECT_TRUE(result_st.is_cancelled());
    EXPECT_EQ(std::string("Set cancelled by MemoryScratchSinkOperator"), result_st.message());
    EXPECT_TRUE(eos);
    EXPECT_EQ(nullptr, record_batch);
    driver->finalize(_runtime_state, DriverState::INTERNAL_ERROR);

    _query_ctx->fragment_mgr()->unregister(fragment_id);
    _query_ctx->count_down_fragments();
}

} // namespace starrocks::pipeline
