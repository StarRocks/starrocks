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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/data_sink.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/data_sink.h"

#include <algorithm>
#include <map>
#include <memory>

#include "common/logging.h"
#include "exec/exec_node.h"
#include "exec/file_builder.h"
#include "exec/pipeline/exchange/exchange_sink_operator.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_executor.h"
#include "exec/pipeline/olap_table_sink_operator.h"
#include "exec/pipeline/result_sink_operator.h"
#include "exec/pipeline/sink/blackhole_table_sink_operator.h"
#include "exec/pipeline/sink/dictionary_cache_sink_operator.h"
#include "exec/pipeline/sink/export_sink_operator.h"
#include "exec/pipeline/sink/file_sink_operator.h"
#include "exec/pipeline/sink/hive_table_sink_operator.h"
#include "exec/pipeline/sink/iceberg_table_sink_operator.h"
#include "exec/pipeline/sink/memory_scratch_sink_operator.h"
#include "exec/pipeline/sink/mysql_table_sink_operator.h"
#include "exec/pipeline/sink/table_function_table_sink_operator.h"
#include "exec/tablet_sink.h"
#include "exprs/expr.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/blackhole_table_sink.h"
#include "runtime/data_stream_sender.h"
#include "runtime/dictionary_cache_sink.h"
#include "runtime/export_sink.h"
#include "runtime/hive_table_sink.h"
#include "runtime/iceberg_table_sink.h"
#include "runtime/memory_scratch_sink.h"
#include "runtime/multi_cast_data_stream_sink.h"
#include "runtime/mysql_table_sink.h"
#include "runtime/result_sink.h"
#include "runtime/runtime_state.h"
#include "runtime/schema_table_sink.h"
#include "runtime/table_function_table_sink.h"

namespace starrocks {

static std::unique_ptr<DataStreamSender> create_data_stream_sink(
        RuntimeState* state, const TDataStreamSink& data_stream_sink, const RowDescriptor& row_desc,
        const TPlanFragmentExecParams& params, int32_t sender_id,
        const std::vector<TPlanFragmentDestination>& destinations) {
    bool send_query_statistics_with_every_batch =
            params.__isset.send_query_statistics_with_every_batch && params.send_query_statistics_with_every_batch;
    bool enable_exchange_pass_through =
            params.__isset.enable_exchange_pass_through && params.enable_exchange_pass_through;
    bool enable_exchange_perf = params.__isset.enable_exchange_perf && params.enable_exchange_perf;

    // TODO: figure out good buffer size based on size of output row
    auto ret = std::make_unique<DataStreamSender>(state, sender_id, row_desc, data_stream_sink, destinations, 16 * 1024,
                                                  send_query_statistics_with_every_batch, enable_exchange_pass_through,
                                                  enable_exchange_perf);
    return ret;
}

Status DataSink::create_data_sink(RuntimeState* state, const TDataSink& thrift_sink,
                                  const std::vector<TExpr>& output_exprs, const TPlanFragmentExecParams& params,
                                  int32_t sender_id, const RowDescriptor& row_desc, std::unique_ptr<DataSink>* sink) {
    DCHECK(sink != nullptr);
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        if (!thrift_sink.__isset.stream_sink) {
            return Status::InternalError("Missing data stream sink.");
        }
        *sink = create_data_stream_sink(state, thrift_sink.stream_sink, row_desc, params, sender_id,
                                        params.destinations);
        break;
    }
    case TDataSinkType::RESULT_SINK:
        if (!thrift_sink.__isset.result_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }

        // TODO: figure out good buffer size based on size of output row
        *sink = std::make_unique<ResultSink>(row_desc, output_exprs, thrift_sink.result_sink, 1024);
        break;
    case TDataSinkType::MEMORY_SCRATCH_SINK:
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }
        *sink = std::make_unique<MemoryScratchSink>(row_desc, output_exprs, thrift_sink.memory_scratch_sink);
        break;
    case TDataSinkType::MYSQL_TABLE_SINK: {
        if (!thrift_sink.__isset.mysql_table_sink) {
            return Status::InternalError("Missing data buffer sink.");
        }
        // TODO: figure out good buffer size based on size of output row
        *sink = std::make_unique<MysqlTableSink>(state->obj_pool(), row_desc, output_exprs);
        break;
    }

    case TDataSinkType::EXPORT_SINK: {
        if (!thrift_sink.__isset.export_sink) {
            return Status::InternalError("Missing export sink sink.");
        }
        *sink = std::make_unique<ExportSink>(state->obj_pool(), row_desc, output_exprs);
        break;
    }
    case TDataSinkType::OLAP_TABLE_SINK: {
        Status status;
        DCHECK(thrift_sink.__isset.olap_table_sink);
        *sink = std::make_unique<stream_load::OlapTableSink>(state->obj_pool(), output_exprs, &status, state);
        RETURN_IF_ERROR(status);
        break;
    }
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK: {
        DCHECK(thrift_sink.__isset.multi_cast_stream_sink || thrift_sink.multi_cast_stream_sink.sinks.size() == 0)
                << "Missing mcast stream sink.";
        auto mcast_data_stream_sink = std::make_unique<MultiCastDataStreamSink>(state);
        const auto& thrift_mcast_stream_sink = thrift_sink.multi_cast_stream_sink;

        for (size_t i = 0; i < thrift_mcast_stream_sink.sinks.size(); i++) {
            const auto& sink = thrift_mcast_stream_sink.sinks[i];
            const auto& destinations = thrift_mcast_stream_sink.destinations[i];
            auto ret = create_data_stream_sink(state, sink, row_desc, params, sender_id, destinations);
            mcast_data_stream_sink->add_data_stream_sink(std::move(ret));
        }
        *sink = std::move(mcast_data_stream_sink);
        break;
    }
    case TDataSinkType::SCHEMA_TABLE_SINK: {
        if (!thrift_sink.__isset.schema_table_sink) {
            return Status::InternalError("Missing schema table sink.");
        }
        *sink = std::make_unique<SchemaTableSink>(state->obj_pool(), row_desc, output_exprs);
        break;
    }
    case TDataSinkType::ICEBERG_TABLE_SINK: {
        if (!thrift_sink.__isset.iceberg_table_sink) {
            return Status::InternalError("Missing iceberg table sink");
        }
        *sink = std::make_unique<IcebergTableSink>(state->obj_pool(), output_exprs);
        break;
    }
    case TDataSinkType::HIVE_TABLE_SINK: {
        if (!thrift_sink.__isset.hive_table_sink) {
            return Status::InternalError("Missing hive table sink");
        }
        *sink = std::make_unique<HiveTableSink>(state->obj_pool(), output_exprs);
        break;
    }
    case TDataSinkType::TABLE_FUNCTION_TABLE_SINK: {
        if (!thrift_sink.__isset.table_function_table_sink) {
            return Status::InternalError("Missing table function table sink");
        }
        *sink = std::make_unique<TableFunctionTableSink>(state->obj_pool(), output_exprs);
        break;
    }
    case TDataSinkType::BLACKHOLE_TABLE_SINK: {
        *sink = std::make_unique<BlackHoleTableSink>(state->obj_pool());
        break;
    }
    case TDataSinkType::DICTIONARY_CACHE_SINK: {
        if (!thrift_sink.__isset.dictionary_cache_sink) {
            return Status::InternalError("Missing dictionary cache sink");
        }
        if (!state->enable_pipeline_engine()) {
            return Status::InternalError("dictionary cache only support pipeline engine");
        }
        *sink = std::make_unique<DictionaryCacheSink>();
        break;
    }

    default:
        std::stringstream error_msg;
        auto i = _TDataSinkType_VALUES_TO_NAMES.find(thrift_sink.type);
        const char* str = "Unknown data sink type ";

        if (i != _TDataSinkType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        error_msg << str << " not implemented.";
        return Status::InternalError(error_msg.str());
    }

    if (*sink != nullptr) {
        RETURN_IF_ERROR((*sink)->init(thrift_sink, state));
    }

    return Status::OK();
}

Status DataSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    return Status::OK();
}

Status DataSink::prepare(RuntimeState* state) {
    _runtime_state = state;
    return Status::OK();
}

Status DataSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::NotSupported("Don't support vector query engine");
}

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wpotentially-evaluated-expression")
#endif
Status DataSink::decompose_data_sink_to_pipeline(pipeline::PipelineBuilderContext* context, RuntimeState* runtime_state,
                                                 pipeline::OpFactories prev_operators,
                                                 const pipeline::UnifiedExecPlanFragmentParams& request,
                                                 const TDataSink& thrift_sink, const std::vector<TExpr>& output_exprs) {
    using namespace pipeline;
    auto fragment_ctx = context->fragment_context();
    size_t dop = context->source_operator(prev_operators)->degree_of_parallelism();
    // TODO: port the following code to detail DataSink subclasses
    if (typeid(*this) == typeid(starrocks::ResultSink)) {
        auto* result_sink = down_cast<starrocks::ResultSink*>(this);
        // Accumulate chunks before sending to result sink
        if (runtime_state->query_options().__isset.enable_result_sink_accumulate &&
            runtime_state->query_options().enable_result_sink_accumulate) {
            ExecNode::may_add_chunk_accumulate_operator(prev_operators, context,
                                                        Operator::s_pseudo_plan_node_id_for_final_sink);
        }
        // Result sink doesn't have plan node id;
        OpFactoryPtr op = nullptr;
        if (result_sink->get_sink_type() == TResultSinkType::FILE) {
            op = std::make_shared<FileSinkOperatorFactory>(context->next_operator_id(), result_sink->get_output_exprs(),
                                                           result_sink->get_file_opts(), dop, fragment_ctx);
        } else {
            op = std::make_shared<ResultSinkOperatorFactory>(
                    context->next_operator_id(), result_sink->get_sink_type(), result_sink->isBinaryFormat(),
                    result_sink->get_format_type(), result_sink->get_output_exprs(), fragment_ctx);
        }
        // Add result sink operator to last pipeline
        prev_operators.emplace_back(op);
        context->add_pipeline(std::move(prev_operators));

    } else if (typeid(*this) == typeid(starrocks::BlackHoleTableSink)) {
        OpFactoryPtr op = std::make_shared<BlackHoleTableSinkOperatorFactory>(context->next_operator_id());
        prev_operators.emplace_back(op);
        context->add_pipeline(prev_operators);
    } else if (typeid(*this) == typeid(starrocks::DataStreamSender)) {
        auto* sender = down_cast<starrocks::DataStreamSender*>(this);
        auto& t_stream_sink = request.output_sink().stream_sink;

        auto exchange_sink = _create_exchange_sink_operator(context, t_stream_sink, sender, dop);

        prev_operators.emplace_back(exchange_sink);
        context->add_pipeline(std::move(prev_operators));
    } else if (typeid(*this) == typeid(starrocks::MultiCastDataStreamSink)) {
        // note(yan): steps are:
        // 1. create exchange[EX]
        // 2. create sink[A] at the end of current pipeline
        // 3. create source[B]/sink[C] pipelines.
        // A -> EX -> B0/C0
        //       | -> B1/C1
        //       | -> B2/C2
        // sink[A] will push chunk to exchanger
        // and source[B] will pull chunk from exchanger
        // so basically you can think exchanger is a chunk repository.
        // Further workflow explanation is in mcast_local_exchange.h file.
        auto* mcast_sink = down_cast<starrocks::MultiCastDataStreamSink*>(this);
        const auto& sinks = mcast_sink->get_sinks();
        auto& t_multi_case_stream_sink = request.output_sink().multi_cast_stream_sink;

        // === create exchange ===
        auto mcast_local_exchanger = std::make_shared<MultiCastLocalExchanger>(runtime_state, sinks.size());

        // === create sink op ====
        auto* upstream = prev_operators.back().get();
        auto* upstream_source = context->source_operator(prev_operators);
        size_t upstream_plan_node_id = upstream->plan_node_id();
        OpFactoryPtr sink_op = std::make_shared<MultiCastLocalExchangeSinkOperatorFactory>(
                context->next_operator_id(), upstream_plan_node_id, mcast_local_exchanger);
        prev_operators.emplace_back(sink_op);
        context->add_pipeline(std::move(prev_operators));

        // ==== create source/sink pipelines ====
        for (size_t i = 0; i < sinks.size(); i++) {
            const auto& sender = sinks[i];
            OpFactories ops;
            // it's okary to set arbitrary dop.
            const size_t dop = 1;
            auto& t_stream_sink = t_multi_case_stream_sink.sinks[i];

            // source op
            auto source_op = std::make_shared<MultiCastLocalExchangeSourceOperatorFactory>(
                    context->next_operator_id(), upstream_plan_node_id, i, mcast_local_exchanger);
            context->inherit_upstream_source_properties(source_op.get(), upstream_source);
            source_op->set_degree_of_parallelism(dop);

            // sink op
            auto sink_op = _create_exchange_sink_operator(context, t_stream_sink, sender.get(), dop);

            ops.emplace_back(source_op);
            ops.emplace_back(sink_op);
            context->add_pipeline(std::move(ops));
        }
    } else if (typeid(*this) == typeid(starrocks::stream_load::OlapTableSink)) {
        size_t desired_tablet_sink_dop = request.pipeline_sink_dop();
        DCHECK(desired_tablet_sink_dop > 0);
        runtime_state->set_num_per_fragment_instances(request.common().params.num_senders);
        std::vector<std::unique_ptr<starrocks::stream_load::OlapTableSink>> tablet_sinks;
        for (int i = 1; i < desired_tablet_sink_dop; i++) {
            Status st;
            std::unique_ptr<starrocks::stream_load::OlapTableSink> sink =
                    std::make_unique<starrocks::stream_load::OlapTableSink>(runtime_state->obj_pool(), output_exprs,
                                                                            &st, runtime_state);
            RETURN_IF_ERROR(st);
            if (sink != nullptr) {
                RETURN_IF_ERROR(sink->init(thrift_sink, runtime_state));
            }
            tablet_sinks.emplace_back(std::move(sink));
        }
        OpFactoryPtr tablet_sink_op = std::make_shared<OlapTableSinkOperatorFactory>(
                context->next_operator_id(), this, fragment_ctx, request.sender_id(), desired_tablet_sink_dop,
                tablet_sinks);
        // FE will pre-set the parallelism for all fragment instance which contains the tablet sink,
        // For stream load, routine load or broker load, the desired_tablet_sink_dop set
        // by FE is same as the dop.
        // For insert into select, in the simplest case like insert into table select * from table2;
        // the desired_tablet_sink_dop set by FE is same as the dop.
        // However, if the select statement is complex, like insert into table select * from table2 limit 1,
        // the desired_tablet_sink_dop set by FE is not same as the dop, and it needs to
        // add a local passthrough exchange here
        if (desired_tablet_sink_dop != dop) {
            auto ops = context->maybe_interpolate_local_passthrough_exchange(
                    runtime_state, Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators,
                    desired_tablet_sink_dop);
            ops.emplace_back(std::move(tablet_sink_op));
            context->add_pipeline(std::move(ops));
        } else {
            prev_operators.emplace_back(std::move(tablet_sink_op));
            context->add_pipeline(std::move(prev_operators));
        }
    } else if (typeid(*this) == typeid(starrocks::ExportSink)) {
        auto* export_sink = down_cast<starrocks::ExportSink*>(this);
        auto output_expr = export_sink->get_output_expr();
        OpFactoryPtr op = std::make_shared<ExportSinkOperatorFactory>(
                context->next_operator_id(), request.output_sink().export_sink, export_sink->get_output_expr(), dop,
                fragment_ctx);
        prev_operators.emplace_back(op);
        context->add_pipeline(std::move(prev_operators));
    } else if (typeid(*this) == typeid(starrocks::MysqlTableSink)) {
        auto* mysql_table_sink = down_cast<starrocks::MysqlTableSink*>(this);
        auto output_expr = mysql_table_sink->get_output_expr();
        OpFactoryPtr op = std::make_shared<MysqlTableSinkOperatorFactory>(
                context->next_operator_id(), request.output_sink().mysql_table_sink,
                mysql_table_sink->get_output_expr(), dop, fragment_ctx);
        prev_operators.emplace_back(op);
        context->add_pipeline(std::move(prev_operators));
    } else if (typeid(*this) == typeid(starrocks::MemoryScratchSink)) {
        auto* memory_scratch_sink = down_cast<starrocks::MemoryScratchSink*>(this);
        auto output_expr = memory_scratch_sink->get_output_expr();
        auto row_desc = memory_scratch_sink->get_row_desc();
        DCHECK_EQ(dop, 1);
        OpFactoryPtr op = std::make_shared<MemoryScratchSinkOperatorFactory>(context->next_operator_id(), row_desc,
                                                                             output_expr, fragment_ctx);

        prev_operators.emplace_back(op);
        context->add_pipeline(std::move(prev_operators));
    } else if (typeid(*this) == typeid(starrocks::IcebergTableSink)) {
        auto* iceberg_table_sink = down_cast<starrocks::IcebergTableSink*>(this);
        TableDescriptor* table_desc =
                runtime_state->desc_tbl().get_table_descriptor(thrift_sink.iceberg_table_sink.target_table_id);
        auto* iceberg_table_desc = down_cast<IcebergTableDescriptor*>(table_desc);

        std::vector<TExpr> partition_expr;
        std::vector<ExprContext*> partition_expr_ctxs;
        auto output_expr = iceberg_table_sink->get_output_expr();
        for (const auto& index : iceberg_table_desc->partition_index_in_schema()) {
            partition_expr.push_back(output_expr[index]);
        }

        RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), partition_expr, &partition_expr_ctxs,
                                                runtime_state));
        size_t desired_iceberg_sink_dop = context->data_sink_dop();

        OpFactoryPtr iceberg_table_sink_op = std::make_shared<IcebergTableSinkOperatorFactory>(
                context->next_operator_id(), fragment_ctx, iceberg_table_sink->get_output_expr(), iceberg_table_desc,
                thrift_sink.iceberg_table_sink, partition_expr_ctxs);

        if (iceberg_table_desc->is_unpartitioned_table() || thrift_sink.iceberg_table_sink.is_static_partition_sink) {
            auto ops = context->maybe_interpolate_local_passthrough_exchange(
                    runtime_state, Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators,
                    desired_iceberg_sink_dop);
            ops.emplace_back(std::move(iceberg_table_sink_op));
            context->add_pipeline(std::move(ops));
        } else {
            auto ops = context->interpolate_local_key_partition_exchange(
                    runtime_state, Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, partition_expr_ctxs,
                    desired_iceberg_sink_dop);
            ops.emplace_back(std::move(iceberg_table_sink_op));
            context->add_pipeline(std::move(ops));
        }

    } else if (typeid(*this) == typeid(starrocks::HiveTableSink)) {
        auto* hive_table_sink = down_cast<starrocks::HiveTableSink*>(this);
        auto output_expr = hive_table_sink->get_output_expr();
        const auto& t_hive_sink = thrift_sink.hive_table_sink;
        const auto partition_col_size = t_hive_sink.partition_column_names.size();
        std::vector<TExpr> partition_expr(output_exprs.end() - partition_col_size, output_exprs.end());
        std::vector<ExprContext*> partition_expr_ctxs;

        RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), partition_expr, &partition_expr_ctxs,
                                                runtime_state));

        size_t desired_hive_sink_dop = context->data_sink_dop();
        OpFactoryPtr hive_table_sink_op = std::make_shared<HiveTableSinkOperatorFactory>(
                context->next_operator_id(), fragment_ctx, thrift_sink.hive_table_sink,
                hive_table_sink->get_output_expr(), partition_expr_ctxs);

        if (t_hive_sink.partition_column_names.size() == 0 || t_hive_sink.is_static_partition_sink) {
            auto ops = context->maybe_interpolate_local_passthrough_exchange(
                    runtime_state, Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators,
                    desired_hive_sink_dop);
            ops.emplace_back(std::move(hive_table_sink_op));
            context->add_pipeline(std::move(ops));
        } else {
            auto ops = context->interpolate_local_key_partition_exchange(
                    runtime_state, Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, partition_expr_ctxs,
                    desired_hive_sink_dop);
            ops.emplace_back(std::move(hive_table_sink_op));
            context->add_pipeline(std::move(ops));
        }
    } else if (typeid(*this) == typeid(starrocks::TableFunctionTableSink)) {
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

        std::vector<TExpr> partition_exprs;
        std::vector<std::string> partition_column_names;
        if (target_table.__isset.partition_column_ids) {
            for (auto id : target_table.partition_column_ids) {
                partition_exprs.push_back(output_exprs[id]);
                partition_column_names.push_back(target_table.columns[id].column_name);
            }
        }
        std::vector<ExprContext*> partition_expr_ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), partition_exprs, &partition_expr_ctxs,
                                                runtime_state));

        std::vector<ExprContext*> output_expr_ctxs;
        RETURN_IF_ERROR(
                Expr::create_expr_trees(runtime_state->obj_pool(), output_exprs, &output_expr_ctxs, runtime_state));

        auto op = std::make_shared<TableFunctionTableSinkOperatorFactory>(
                context->next_operator_id(), target_table.path, target_table.file_format, target_table.compression_type,
                output_expr_ctxs, partition_expr_ctxs, column_names, partition_column_names,
                target_table.write_single_file, thrift_sink.table_function_table_sink.cloud_configuration,
                fragment_ctx);

        size_t sink_dop = context->data_sink_dop();
        if (target_table.write_single_file) {
            sink_dop = 1;
        }

        if (partition_expr_ctxs.empty()) {
            auto ops = context->maybe_interpolate_local_passthrough_exchange(
                    runtime_state, Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, sink_dop);
            ops.emplace_back(std::move(op));
            context->add_pipeline(std::move(ops));

        } else {
            auto ops = context->interpolate_local_key_partition_exchange(runtime_state,
                                                                         Operator::s_pseudo_plan_node_id_for_final_sink,
                                                                         prev_operators, partition_expr_ctxs, sink_dop);
            ops.emplace_back(std::move(op));
            context->add_pipeline(std::move(ops));
        }
    } else if (typeid(*this) == typeid(starrocks::DictionaryCacheSink)) {
        OpFactoryPtr op = std::make_shared<DictionaryCacheSinkOperatorFactory>(
                context->next_operator_id(), request.output_sink().dictionary_cache_sink, fragment_ctx);

        prev_operators.emplace_back(op);
        context->add_pipeline(std::move(prev_operators));
    } else {
        return Status::InternalError(fmt::format("Unknown data sink type: {}", typeid(*this).name()));
    }
    return Status::OK();
}
DIAGNOSTIC_POP

OperatorFactoryPtr DataSink::_create_exchange_sink_operator(pipeline::PipelineBuilderContext* context,
                                                            const TDataStreamSink& stream_sink,
                                                            const DataStreamSender* sender, size_t dop) {
    using namespace pipeline;
    auto fragment_ctx = context->fragment_context();

    bool is_dest_merge = stream_sink.__isset.is_merge && stream_sink.is_merge;

    bool is_pipeline_level_shuffle = false;
    int32_t dest_dop = 1;
    bool enable_pipeline_level_shuffle = context->runtime_state()->query_ctx()->enable_pipeline_level_shuffle();
    if (enable_pipeline_level_shuffle &&
        (sender->get_partition_type() == TPartitionType::HASH_PARTITIONED ||
         sender->get_partition_type() == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED)) {
        is_pipeline_level_shuffle = true;
        dest_dop = stream_sink.dest_dop;
        DCHECK_GT(dest_dop, 0);
    }

    std::shared_ptr<SinkBuffer> sink_buffer =
            std::make_shared<SinkBuffer>(fragment_ctx, sender->destinations(), is_dest_merge);

    auto exchange_sink = std::make_shared<ExchangeSinkOperatorFactory>(
            context->next_operator_id(), stream_sink.dest_node_id, sink_buffer, sender->get_partition_type(),
            sender->destinations(), is_pipeline_level_shuffle, dest_dop, sender->sender_id(),
            sender->get_dest_node_id(), sender->get_partition_exprs(),
            !is_dest_merge && sender->get_enable_exchange_pass_through(),
            sender->get_enable_exchange_perf() && !context->has_aggregation, fragment_ctx, sender->output_columns());
    return exchange_sink;
}

} // namespace starrocks
