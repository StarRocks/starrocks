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

#include "table_function_table_sink.h"

#include "common/logging.h"
#include "connector/file_chunk_sink.h"
#include "connector/hive_chunk_sink.h"
#include "exec/data_sink.h"
#include "exec/hdfs_scanner_text.h"
#include "exec/pipeline/sink/connector_sink_operator.h"
#include "exprs/expr.h"
#include "formats/column_evaluator.h"
#include "formats/csv/csv_file_writer.h"
#include "glog/logging.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

TableFunctionTableSink::TableFunctionTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

TableFunctionTableSink::~TableFunctionTableSink() = default;

Status TableFunctionTableSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(thrift_sink, state));
    RETURN_IF_ERROR(prepare(state));
    RETURN_IF_ERROR(open(state));
    return Status::OK();
}

Status TableFunctionTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "TableFunctionTableSink (frag_id=" << state->fragment_instance_id() << ")";
    _profile = _pool->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status TableFunctionTableSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    return Status::OK();
}

Status TableFunctionTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::OK();
}

Status TableFunctionTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}

Status TableFunctionTableSink::decompose_to_pipeline(pipeline::OpFactories prev_operators, const TDataSink& thrift_sink,
                                                     pipeline::PipelineBuilderContext* context) const {
    auto runtime_state = context->runtime_state();
    auto fragment_ctx = context->fragment_context();
    auto output_exprs = this->get_output_expr();

    DCHECK(thrift_sink.table_function_table_sink.__isset.target_table);
    DCHECK(thrift_sink.table_function_table_sink.__isset.cloud_configuration);

    const auto& target_table = thrift_sink.table_function_table_sink.target_table;
    DCHECK(target_table.__isset.path);
    DCHECK(target_table.__isset.file_format);
    DCHECK(target_table.__isset.columns);
    DCHECK(target_table.__isset.write_single_file);
    DCHECK(target_table.columns.size() == output_exprs.size());

    std::vector<std::string> column_names;
    for (const auto& column : target_table.columns) {
        column_names.push_back(column.column_name);
    }

    // prepare sink context
    auto sink_ctx = std::make_shared<connector::FileChunkSinkContext>();
    sink_ctx->path = target_table.path;
    sink_ctx->cloud_conf = thrift_sink.table_function_table_sink.cloud_configuration;
    sink_ctx->column_names = std::move(column_names);
    if (target_table.__isset.partition_column_ids) {
        sink_ctx->partition_column_indices = target_table.partition_column_ids;
    }
    sink_ctx->executor = ExecEnv::GetInstance()->pipeline_sink_io_pool();
    sink_ctx->format = target_table.file_format;
    if (target_table.__isset.target_max_file_size) {
        sink_ctx->max_file_size = target_table.target_max_file_size;
    }
    if (target_table.__isset.write_single_file && target_table.write_single_file) {
        sink_ctx->max_file_size = INT64_MAX;
    }
    sink_ctx->compression_type = target_table.compression_type;
    sink_ctx->column_evaluators = ColumnExprEvaluator::from_exprs(output_exprs, runtime_state);
    sink_ctx->fragment_context = fragment_ctx;
    if (target_table.__isset.csv_column_seperator) {
        sink_ctx->options[formats::CSVWriterOptions::COLUMN_TERMINATED_BY] = target_table.csv_column_seperator;
    }
    if (target_table.__isset.csv_row_delimiter) {
        sink_ctx->options[formats::CSVWriterOptions::LINE_TERMINATED_BY] = target_table.csv_row_delimiter;
    }
    if (target_table.__isset.parquet_use_legacy_encoding && target_table.parquet_use_legacy_encoding) {
        sink_ctx->options[formats::ParquetWriterOptions::USE_LEGACY_DECIMAL_ENCODING] = "true";
        sink_ctx->options[formats::ParquetWriterOptions::USE_INT96_TIMESTAMP_ENCODING] = "true";
    }

    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::FILE);
    auto sink_provider = connector->create_data_sink_provider();
    auto op = std::make_shared<pipeline::ConnectorSinkOperatorFactory>(
            context->next_operator_id(), std::move(sink_provider), sink_ctx, fragment_ctx);

    size_t sink_dop = target_table.write_single_file ? 1 : context->data_sink_dop();
    if (sink_ctx->partition_column_indices.empty()) {
        auto ops = context->maybe_interpolate_local_passthrough_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, sink_dop,
                pipeline::LocalExchanger::PassThroughType::SCALE);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));

    } else {
        std::vector<TExpr> partition_exprs;
        for (auto id : target_table.partition_column_ids) {
            partition_exprs.push_back(output_exprs[id]);
        }
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
