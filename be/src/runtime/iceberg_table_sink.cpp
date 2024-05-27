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

#include "iceberg_table_sink.h"

#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

IcebergTableSink::IcebergTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

IcebergTableSink::~IcebergTableSink() = default;

Status IcebergTableSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(thrift_sink, state));
    RETURN_IF_ERROR(prepare(state));
    RETURN_IF_ERROR(open(state));
    return Status::OK();
}

Status IcebergTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "IcebergTableSink (frag_id=" << state->fragment_instance_id() << ")";
    _profile = _pool->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status IcebergTableSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    return Status::OK();
}

Status IcebergTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::OK();
}

Status IcebergTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}

Status IcebergTableSink::decompose_to_pipeline(pipeline::OpFactories prev_operators, const TDataSink& thrift_sink,
                                               pipeline::PipelineBuilderContext* context) const {
    auto* runtime_state = context->runtime_state();
    auto* fragment_ctx = context->fragment_context();
    TableDescriptor* table_desc =
            runtime_state->desc_tbl().get_table_descriptor(thrift_sink.iceberg_table_sink.target_table_id);
    auto* iceberg_table_desc = down_cast<IcebergTableDescriptor*>(table_desc);
    auto& t_iceberg_sink = thrift_sink.iceberg_table_sink;

    auto sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
    sink_ctx->path = t_iceberg_sink.location + connector::IcebergUtils::DATA_DIRECTORY;
    sink_ctx->cloud_conf = t_iceberg_sink.cloud_configuration;
    sink_ctx->column_names = iceberg_table_desc->full_column_names();
    sink_ctx->partition_column_indices = iceberg_table_desc->partition_index_in_schema();
    sink_ctx->executor = ExecEnv::GetInstance()->pipeline_sink_io_pool();
    sink_ctx->format = t_iceberg_sink.file_format; // iceberg sink only supports parquet
    sink_ctx->compression_type = t_iceberg_sink.compression_type;
    if (t_iceberg_sink.__isset.target_max_file_size) {
        sink_ctx->max_file_size = t_iceberg_sink.target_max_file_size;
    }
    sink_ctx->parquet_field_ids =
            connector::IcebergUtils::generate_parquet_field_ids(iceberg_table_desc->get_iceberg_schema()->fields);
    sink_ctx->column_evaluators = ColumnExprEvaluator::from_exprs(this->get_output_expr(), runtime_state);
    sink_ctx->fragment_context = fragment_ctx;

    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::ICEBERG);
    auto sink_provider = connector->create_data_sink_provider();
    auto op = std::make_shared<pipeline::ConnectorSinkOperatorFactory>(
            context->next_operator_id(), std::move(sink_provider), sink_ctx, fragment_ctx);
    size_t sink_dop = context->data_sink_dop();

    if (iceberg_table_desc->is_unpartitioned_table() || t_iceberg_sink.is_static_partition_sink) {
        auto ops = context->maybe_interpolate_local_passthrough_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, sink_dop,
                pipeline::LocalExchanger::PassThroughType::SCALE);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    } else {
        std::vector<TExpr> partition_expr;
        std::vector<ExprContext*> partition_expr_ctxs;
        auto output_expr = this->get_output_expr();
        for (const auto& index : iceberg_table_desc->partition_index_in_schema()) {
            partition_expr.push_back(output_expr[index]);
        }

        RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), partition_expr, &partition_expr_ctxs,
                                                runtime_state));
        auto ops = context->interpolate_local_key_partition_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators,
                partition_expr_ctxs, sink_dop);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    }

    return Status::OK();
}

} // namespace starrocks
