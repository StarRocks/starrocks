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

#include "exec/pipeline/sink/table_function_table_sink_operator.h"

#include "gen_cpp/RuntimeProfile_types.h"
#include "gtest/gtest.h"

namespace starrocks::pipeline {
class TableFunctionTableSinkOperatorTest : public testing::Test {
public:
    TableFunctionTableSinkOperatorTest() : _runtime_state(TQueryGlobals()) {}

protected:
    void SetUp() override;

private:
    RuntimeState _runtime_state;
    DescriptorTbl* _desc_tbl = nullptr;
    TPlanNode _tnode;
};

void TableFunctionTableSinkOperatorTest::SetUp() {
    _runtime_state.set_desc_tbl(_desc_tbl);
    _tnode.node_id = 1;
    _tnode.node_type = TPlanNodeType::TABLE_FUNCTION_NODE;
    _tnode.num_children = 1;
}

TEST_F(TableFunctionTableSinkOperatorTest, prepare_with_parquet_format) {
    TDataSink dataSink;
    const auto& target_table = dataSink.table_function_table_sink.target_table;

    std::vector<TExpr> partition_exprs;
    std::vector<std::string> partition_column_names;

    std::vector<ExprContext*> partition_expr_ctxs;
    std::vector<ExprContext*> output_expr_ctxs;
    std::vector<std::string> column_names;

    auto result = parquet::ParquetBuildHelper::make_schema(column_names, output_expr_ctxs,
                                                           std::vector<parquet::FileColumnId>(output_expr_ctxs.size()));

    std::string format = "parquet";

    auto _parquet_file_schema = result.ValueOrDie();

    TExecPlanFragmentParams _request;

    const auto& params = _request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    pipeline::QueryContext* _query_ctx;

    pipeline::FragmentContext* _fragment_ctx;
    ExecEnv* _exec_env = ExecEnv::GetInstance();

    _query_ctx = _exec_env->query_context_mgr()->get_or_register(query_id);
    _query_ctx->set_query_id(query_id);
    _query_ctx->set_total_fragments(1);
    _query_ctx->set_delivery_expire_seconds(600);
    _query_ctx->set_query_expire_seconds(600);
    _query_ctx->extend_delivery_lifetime();
    _query_ctx->extend_query_lifetime();
    _query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                 GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                           _request.query_options, _request.query_globals, _exec_env));
    _fragment_ctx->set_is_stream_pipeline(true);
    _fragment_ctx->set_is_stream_test(true);

    TableFunctionTableSinkOperatorFactory factory(
            1, target_table.path, format, target_table.compression_type, output_expr_ctxs, partition_expr_ctxs,
            column_names, partition_column_names, target_table.write_single_file,
            dataSink.table_function_table_sink.cloud_configuration, _fragment_ctx);

    ASSERT_TRUE(factory.prepare(&_runtime_state).ok());
}

TEST_F(TableFunctionTableSinkOperatorTest, prepare_with_orc_format) {
    TDataSink dataSink;
    const auto& target_table = dataSink.table_function_table_sink.target_table;

    std::vector<TExpr> partition_exprs;
    std::vector<std::string> partition_column_names;

    std::vector<ExprContext*> partition_expr_ctxs;
    std::vector<ExprContext*> output_expr_ctxs;
    std::vector<std::string> column_names;

    auto result = parquet::ParquetBuildHelper::make_schema(column_names, output_expr_ctxs,
                                                           std::vector<parquet::FileColumnId>(output_expr_ctxs.size()));

    std::string format = "orc";

    auto _parquet_file_schema = result.ValueOrDie();

    TExecPlanFragmentParams _request;

    const auto& params = _request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    pipeline::QueryContext* _query_ctx;

    pipeline::FragmentContext* _fragment_ctx;
    ExecEnv* _exec_env = ExecEnv::GetInstance();

    _query_ctx = _exec_env->query_context_mgr()->get_or_register(query_id);
    _query_ctx->set_query_id(query_id);
    _query_ctx->set_total_fragments(1);
    _query_ctx->set_delivery_expire_seconds(600);
    _query_ctx->set_query_expire_seconds(600);
    _query_ctx->extend_delivery_lifetime();
    _query_ctx->extend_query_lifetime();
    _query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                 GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                           _request.query_options, _request.query_globals, _exec_env));
    _fragment_ctx->set_is_stream_pipeline(true);
    _fragment_ctx->set_is_stream_test(true);

    TableFunctionTableSinkOperatorFactory factory(
            1, target_table.path, format, target_table.compression_type, output_expr_ctxs, partition_expr_ctxs,
            column_names, partition_column_names, target_table.write_single_file,
            dataSink.table_function_table_sink.cloud_configuration, _fragment_ctx);

    Status status = factory.prepare(&_runtime_state);

    ASSERT_FALSE(status.ok());

    ASSERT_TRUE(status.code() == TStatusCode::INTERNAL_ERROR);
    ASSERT_TRUE(status.message() == "unsupported file format: orc");
}

} // namespace starrocks::pipeline