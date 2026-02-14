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

#include "common/runtime_profile.h"
#include "exprs/expr.h"
#include "runtime/descriptors_ext.h"
#include "runtime/runtime_state.h"

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

    // Determine if this is a delete sink (delete files) or regular sink (data files)
    bool is_delete_sink = (thrift_sink.type == TDataSinkType::ICEBERG_DELETE_SINK);

    std::unique_ptr<connector::ConnectorChunkSinkProvider> sink_provider;
    std::shared_ptr<connector::ConnectorChunkSinkContext> sink_ctx;
    std::vector<TExpr> partition_expr;
    std::vector<std::string> transform_exprs = iceberg_table_desc->get_transform_exprs();

    if (is_delete_sink) {
        RETURN_IF_ERROR(create_delete_sink_context(thrift_sink, runtime_state, context, iceberg_table_desc,
                                                   sink_provider, sink_ctx, partition_expr));
    } else {
        RETURN_IF_ERROR(create_data_sink_context(thrift_sink, runtime_state, context, iceberg_table_desc, sink_provider,
                                                 sink_ctx, partition_expr));
    }

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
        std::vector<ExprContext*> partition_expr_ctxs;

        RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), partition_expr, &partition_expr_ctxs,
                                                runtime_state));

        // Validate partition expressions were created successfully
        for (int i = 0; i < partition_expr_ctxs.size(); i++) {
            if (partition_expr_ctxs[i] == nullptr) {
                return Status::InternalError(fmt::format("Partition expression at index {} is nullptr", i));
            }
            if (partition_expr_ctxs[i]->root() == nullptr) {
                return Status::InternalError(fmt::format("Partition expression root at index {} is nullptr", i));
            }
        }

        auto ops = context->interpolate_local_key_partition_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators,
                partition_expr_ctxs, sink_dop, transform_exprs);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    }

    return Status::OK();
}

Status IcebergTableSink::create_delete_sink_context(
        const TDataSink& thrift_sink, RuntimeState* runtime_state, pipeline::PipelineBuilderContext* context,
        IcebergTableDescriptor* iceberg_table_desc,
        std::unique_ptr<connector::ConnectorChunkSinkProvider>& sink_provider,
        std::shared_ptr<connector::ConnectorChunkSinkContext>& sink_ctx, std::vector<TExpr>& partition_expr) const {
    auto* fragment_ctx = context->fragment_context();
    auto& t_iceberg_sink = thrift_sink.iceberg_table_sink;

    // Create merge sink context for delete files
    auto delete_sink_ctx = std::make_shared<connector::IcebergDeleteSinkContext>();
    delete_sink_ctx->path = t_iceberg_sink.data_location;
    delete_sink_ctx->cloud_configuration = t_iceberg_sink.cloud_configuration;
    delete_sink_ctx->compression_type = t_iceberg_sink.compression_type;
    if (t_iceberg_sink.__isset.target_max_file_size) {
        delete_sink_ctx->max_file_size = t_iceberg_sink.target_max_file_size;
    }
    delete_sink_ctx->tuple_desc_id = t_iceberg_sink.tuple_id;
    delete_sink_ctx->executor = ExecEnv::GetInstance()->pipeline_sink_io_pool();
    delete_sink_ctx->fragment_context = fragment_ctx;

    // Delete files have columns: file_path and pos (row_position)
    const auto& output_exprs = this->get_output_expr();
    delete_sink_ctx->column_evaluators = ColumnExprEvaluator::from_exprs(output_exprs, runtime_state);
    delete_sink_ctx->transform_exprs = iceberg_table_desc->get_transform_exprs();
    delete_sink_ctx->output_exprs = output_exprs;

    // Configure partition columns if table is partitioned
    delete_sink_ctx->partition_column_names = iceberg_table_desc->partition_column_names();

    // Build column name to slot reference map for partition column updates
    TupleDescriptor* tuple_desc = runtime_state->desc_tbl().get_tuple_descriptor(t_iceberg_sink.tuple_id);
    if (tuple_desc == nullptr) {
        return Status::InternalError(
                fmt::format("Failed to find tuple descriptor with id {}", t_iceberg_sink.tuple_id));
    }

    const auto& slots = tuple_desc->slots();
    if (slots.size() != output_exprs.size()) {
        return Status::InternalError(fmt::format("Mismatched slot and output expression counts: {} vs {}", slots.size(),
                                                 output_exprs.size()));
    }

    for (size_t i = 0; i < slots.size(); ++i) {
        const auto* slot = slots[i];
        const auto& expr = output_exprs[i];
        for (const auto& node : expr.nodes) {
            if (node.node_type == TExprNodeType::SLOT_REF && node.__isset.slot_ref) {
                delete_sink_ctx->column_slot_map[slot->col_name()] = node;
                break;
            }
        }
    }

    if (!iceberg_table_desc->is_unpartitioned_table()) {
        partition_expr = iceberg_table_desc->get_partition_exprs();
        const auto& partition_source_column_names = iceberg_table_desc->partition_source_column_names();

        RETURN_IF_ERROR(update_partition_expr_slot_refs_by_map(partition_expr, delete_sink_ctx->column_slot_map,
                                                               partition_source_column_names));
        delete_sink_ctx->partition_evaluators = ColumnExprEvaluator::from_exprs(partition_expr, runtime_state);
    }

    sink_ctx = delete_sink_ctx;
    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::ICEBERG);
    sink_provider = connector->create_delete_sink_provider();

    return Status::OK();
}

Status IcebergTableSink::create_data_sink_context(const TDataSink& thrift_sink, RuntimeState* runtime_state,
                                                  pipeline::PipelineBuilderContext* context,
                                                  IcebergTableDescriptor* iceberg_table_desc,
                                                  std::unique_ptr<connector::ConnectorChunkSinkProvider>& sink_provider,
                                                  std::shared_ptr<connector::ConnectorChunkSinkContext>& sink_ctx,
                                                  std::vector<TExpr>& partition_expr) const {
    auto* fragment_ctx = context->fragment_context();
    auto& t_iceberg_sink = thrift_sink.iceberg_table_sink;

    auto data_sink_ctx = std::make_shared<connector::IcebergChunkSinkContext>();
    data_sink_ctx->path = t_iceberg_sink.__isset.data_location && !t_iceberg_sink.data_location.empty()
                                  ? t_iceberg_sink.data_location
                                  : t_iceberg_sink.location + connector::IcebergUtils::DATA_DIRECTORY;
    data_sink_ctx->cloud_conf = t_iceberg_sink.cloud_configuration;
    data_sink_ctx->column_names = iceberg_table_desc->full_column_names();
    data_sink_ctx->partition_column_names = iceberg_table_desc->partition_column_names();
    data_sink_ctx->executor = ExecEnv::GetInstance()->pipeline_sink_io_pool();
    data_sink_ctx->format = t_iceberg_sink.file_format; // iceberg sink only supports parquet
    data_sink_ctx->compression_type = t_iceberg_sink.compression_type;
    if (t_iceberg_sink.__isset.target_max_file_size) {
        data_sink_ctx->max_file_size = t_iceberg_sink.target_max_file_size;
    }
    data_sink_ctx->parquet_field_ids =
            connector::IcebergUtils::generate_parquet_field_ids(iceberg_table_desc->get_iceberg_schema()->fields);
    data_sink_ctx->column_evaluators = ColumnExprEvaluator::from_exprs(this->get_output_expr(), runtime_state);
    data_sink_ctx->transform_exprs = iceberg_table_desc->get_transform_exprs();
    data_sink_ctx->fragment_context = fragment_ctx;
    data_sink_ctx->tuple_desc_id = t_iceberg_sink.tuple_id;
    auto& sort_order = iceberg_table_desc->sort_order();
    if (!sort_order.sort_key_idxes.empty()) {
        data_sink_ctx->sort_ordering = std::make_shared<connector::SortOrdering>();
        data_sink_ctx->sort_ordering->sort_key_idxes.assign(sort_order.sort_key_idxes.begin(),
                                                            sort_order.sort_key_idxes.end());
        data_sink_ctx->sort_ordering->sort_descs.descs.reserve(sort_order.sort_key_idxes.size());
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
            data_sink_ctx->sort_ordering->sort_descs.descs.emplace_back(is_asc, is_null_first);
        }
    }

    sink_ctx = data_sink_ctx;
    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::ICEBERG);
    sink_provider = connector->create_data_sink_provider();

    if (iceberg_table_desc->is_unpartitioned_table()) {
        //do nothing
    } else if (t_iceberg_sink.is_static_partition_sink) {
        for (const auto& index : iceberg_table_desc->partition_source_index_in_schema()) {
            if (index < 0 || index >= this->get_output_expr().size()) {
                return Status::InternalError(fmt::format("Invalid partition index: {}", index));
            }
            partition_expr.push_back(this->get_output_expr()[index]);
        }
        data_sink_ctx->partition_evaluators = ColumnExprEvaluator::from_exprs(partition_expr, runtime_state);
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
        data_sink_ctx->partition_evaluators = ColumnExprEvaluator::from_exprs(partition_expr, runtime_state);
    }

    return Status::OK();
}

// Updates partition expression slot references using the column slot map.
// For each partition expression, replaces the slot reference node with the correct
// slot reference from the column_slot_map based on the partition source column name.
//
// Parameters:
//   partition_expr - The partition expressions to update (modified in-place)
//   column_slot_map - Mapping from column name to slot reference node
//   partition_source_column_names - Names of source columns for partitioning
//
// Returns Status::OK() on success, or an error if a required slot reference is missing.
Status IcebergTableSink::update_partition_expr_slot_refs_by_map(
        std::vector<TExpr>& partition_expr, const std::unordered_map<std::string, TExprNode>& column_slot_map,
        const std::vector<std::string>& partition_source_column_names) const {
    // Validate input sizes match
    if (partition_expr.size() != partition_source_column_names.size()) {
        return Status::InternalError(fmt::format("Mismatched partition expression and column name counts: {} vs {}",
                                                 partition_expr.size(), partition_source_column_names.size()));
    }

    // Update each partition expression's slot reference
    for (size_t i = 0; i < partition_expr.size(); ++i) {
        const std::string& source_col_name = partition_source_column_names[i];

        // Find the slot reference for this source column
        auto it = column_slot_map.find(source_col_name);
        if (it == column_slot_map.end()) {
            return Status::InternalError(
                    fmt::format("Could not find slot reference for partition column: {}", source_col_name));
        }

        const TExprNode& slot_info = it->second;
        bool slot_ref_updated = false;

        // Update the slot reference in the partition expression
        for (auto& node : partition_expr[i].nodes) {
            if (node.node_type == TExprNodeType::SLOT_REF && node.__isset.slot_ref) {
                node = slot_info;
                slot_ref_updated = true;
                break;
            }
        }

        if (!slot_ref_updated) {
            return Status::InternalError(
                    fmt::format("No slot reference found in partition expression for column: {}", source_col_name));
        }
    }

    return Status::OK();
}

} // namespace starrocks
