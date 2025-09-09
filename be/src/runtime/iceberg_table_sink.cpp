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
    sink_ctx->partition_column_names = iceberg_table_desc->partition_column_names();
    sink_ctx->executor = ExecEnv::GetInstance()->pipeline_sink_io_pool();
    sink_ctx->format = t_iceberg_sink.file_format; // iceberg sink only supports parquet
    sink_ctx->compression_type = t_iceberg_sink.compression_type;
    if (t_iceberg_sink.__isset.target_max_file_size) {
        sink_ctx->max_file_size = t_iceberg_sink.target_max_file_size;
    }
    sink_ctx->parquet_field_ids =
            connector::IcebergUtils::generate_parquet_field_ids(iceberg_table_desc->get_iceberg_schema()->fields);
    sink_ctx->column_evaluators = ColumnExprEvaluator::from_exprs(this->get_output_expr(), runtime_state);
    sink_ctx->transform_exprs = iceberg_table_desc->get_transform_exprs();
    sink_ctx->fragment_context = fragment_ctx;
    sink_ctx->tuple_desc_id = t_iceberg_sink.tuple_id;
    auto& sort_order = iceberg_table_desc->sort_order();
    if (!sort_order.sort_key_idxes.empty()) {
        sink_ctx->sort_ordering = std::make_shared<connector::SortOrdering>();
        sink_ctx->sort_ordering->sort_key_idxes.assign(sort_order.sort_key_idxes.begin(),
                                                       sort_order.sort_key_idxes.end());
        sink_ctx->sort_ordering->sort_descs.descs.reserve(sort_order.sort_key_idxes.size());
        for (size_t idx = 0; idx < sort_order.sort_key_idxes.size(); ++idx) {
            bool is_asc = idx < sort_order.is_ascs.size() ? sort_order.is_ascs[idx] : true;
            bool is_null_first = false;
            if (idx < sort_order.is_null_firsts.size()) {
                is_null_first = sort_order.is_null_firsts[idx];
            } else if (is_asc) {
                // If ascending, nulls are first by default
                // If descending, nulls are last by default
                is_null_first = true;
            }
            sink_ctx->sort_ordering->sort_descs.descs.emplace_back(is_asc, is_null_first);
        }
    }

    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::ICEBERG);
    auto sink_provider = connector->create_data_sink_provider();
    auto op = std::make_shared<pipeline::ConnectorSinkOperatorFactory>(
            context->next_operator_id(), std::move(sink_provider), sink_ctx, fragment_ctx);
    size_t sink_dop = context->data_sink_dop();

    std::vector<TExpr> partition_expr;
    if (iceberg_table_desc->is_unpartitioned_table()) {
        //do nothing
    } else if (t_iceberg_sink.is_static_partition_sink) {
        for (const auto& index : iceberg_table_desc->partition_source_index_in_schema()) {
            if (index < 0 || index >= this->get_output_expr().size()) {
                return Status::InternalError(fmt::format("Invalid partition index: {}", index));
            }
            partition_expr.push_back(this->get_output_expr()[index]);
        }
        sink_ctx->partition_evaluators = ColumnExprEvaluator::from_exprs(partition_expr, runtime_state);
    } else {
        auto source_column_index = iceberg_table_desc->partition_source_index_in_schema();
        partition_expr = iceberg_table_desc->get_partition_exprs();

        //for 3.5 fe -> 4.0 be compact, try to set this.
        if (partition_expr.empty()) {
            auto output_expr = this->get_output_expr();
            for (const auto& index : source_column_index) {
                if (index < 0 || index >= this->get_output_expr().size()) {
                    return Status::InternalError(fmt::format("Invalid partition index: {}", index));
                }
                partition_expr.push_back(output_expr[index]);
            }
        }

        int idx = 0;
        for (auto& part_expr : partition_expr) {
            int index = source_column_index[idx];
            //check index is valid for output_expr
            if (index < 0 || index >= this->get_output_expr().size()) {
                return Status::InternalError(fmt::format("Invalid partition index: {}", index));
            }
            auto slot_ref = this->get_output_expr()[index];
            for (auto& node : part_expr.nodes) {
                if (node.node_type == TExprNodeType::SLOT_REF) {
                    node = slot_ref.nodes[0];
                    break;
                }
            }
            idx++;
        }
        sink_ctx->partition_evaluators = ColumnExprEvaluator::from_exprs(partition_expr, runtime_state);
    }

    if (iceberg_table_desc->is_unpartitioned_table() || t_iceberg_sink.is_static_partition_sink) {
        auto ops = context->maybe_interpolate_local_passthrough_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, sink_dop,
                pipeline::LocalExchanger::PassThroughType::SCALE);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    } else {
        std::vector<ExprContext*> partition_expr_ctxs;

        RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), partition_expr, &partition_expr_ctxs,
                                                runtime_state));
        auto ops = context->interpolate_local_key_partition_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators,
                partition_expr_ctxs, sink_dop, sink_ctx->transform_exprs);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    }

    return Status::OK();
}

} // namespace starrocks
