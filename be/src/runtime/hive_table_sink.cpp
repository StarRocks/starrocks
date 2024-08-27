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

#include "hive_table_sink.h"

#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

HiveTableSink::HiveTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

HiveTableSink::~HiveTableSink() = default;

Status HiveTableSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(thrift_sink, state));
    RETURN_IF_ERROR(prepare(state));
    RETURN_IF_ERROR(open(state));
    return Status::OK();
}

Status HiveTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "IcebergTableSink (frag_id=" << state->fragment_instance_id() << ")";
    _profile = _pool->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status HiveTableSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    return Status::OK();
}

Status HiveTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::OK();
}

Status HiveTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}

Status HiveTableSink::decompose_to_pipeline(pipeline::OpFactories prev_operators, const TDataSink& thrift_sink,
                                            pipeline::PipelineBuilderContext* context) const {
    auto runtime_state = context->runtime_state();
    auto fragment_ctx = context->fragment_context();
    auto output_exprs = this->get_output_expr();
    const auto& t_hive_sink = thrift_sink.hive_table_sink;
    const auto num_data_columns = t_hive_sink.data_column_names.size();
    std::vector<TExpr> data_exprs(output_exprs.begin(), output_exprs.begin() + num_data_columns);
    std::vector<TExpr> partition_exprs(output_exprs.begin() + num_data_columns, output_exprs.end());

    auto sink_ctx = std::make_shared<connector::HiveChunkSinkContext>();
    sink_ctx->path = t_hive_sink.staging_dir;
    sink_ctx->cloud_conf = t_hive_sink.cloud_configuration;
    sink_ctx->data_column_names = t_hive_sink.data_column_names;
    sink_ctx->partition_column_names = t_hive_sink.partition_column_names;
    sink_ctx->data_column_evaluators = ColumnExprEvaluator::from_exprs(data_exprs, runtime_state);
    sink_ctx->partition_column_evaluators = ColumnExprEvaluator::from_exprs(partition_exprs, runtime_state);
    sink_ctx->executor = ExecEnv::GetInstance()->pipeline_sink_io_pool();
    sink_ctx->format = t_hive_sink.file_format;
    sink_ctx->compression_type = t_hive_sink.compression_type;
    if (t_hive_sink.__isset.target_max_file_size) {
        sink_ctx->max_file_size = t_hive_sink.target_max_file_size;
    }
    if (t_hive_sink.__isset.text_file_desc) {
        DCHECK(boost::iequals(t_hive_sink.file_format, formats::TEXTFILE));
        sink_ctx->options[formats::CSVWriterOptions::COLUMN_TERMINATED_BY] = DEFAULT_FIELD_DELIM;
        sink_ctx->options[formats::CSVWriterOptions::LINE_TERMINATED_BY] = DEFAULT_LINE_DELIM;

        // use customized value if specified
        if (t_hive_sink.text_file_desc.__isset.field_delim) {
            sink_ctx->options[formats::CSVWriterOptions::COLUMN_TERMINATED_BY] = t_hive_sink.text_file_desc.field_delim;
        }
        if (t_hive_sink.text_file_desc.__isset.line_delim) {
            sink_ctx->options[formats::CSVWriterOptions::LINE_TERMINATED_BY] = t_hive_sink.text_file_desc.line_delim;
        }
    }
    sink_ctx->fragment_context = fragment_ctx;

    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::HIVE);
    auto sink_provider = connector->create_data_sink_provider();
    auto op = std::make_shared<pipeline::ConnectorSinkOperatorFactory>(
            context->next_operator_id(), std::move(sink_provider), sink_ctx, fragment_ctx);
    size_t sink_dop = context->data_sink_dop();
    if (t_hive_sink.partition_column_names.size() == 0 || t_hive_sink.is_static_partition_sink) {
        auto ops = context->maybe_interpolate_local_passthrough_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, sink_dop,
                pipeline::LocalExchanger::PassThroughType::SCALE);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    } else {
        std::vector<ExprContext*> partition_expr_ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), partition_exprs, &partition_expr_ctxs,
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
