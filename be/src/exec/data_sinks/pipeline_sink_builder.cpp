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

#include "exec/data_sinks/pipeline_sink_builder.h"

#include <memory>
#include <utility>
#include <vector>

#include "data_sink/exchange/data_stream_sender.h"
#include "data_sink/result/result_sink.h"
#include "data_sink/tablet/multi_olap_table_sink.h"
#include "data_sink/tablet/olap_table_sink.h"
#include "exec/data_sinks/hive_sink_builder.h"
#include "exec/data_sinks/table_function_sink_builder.h"
#include "exec/pipeline/exchange/exchange_sink_operator.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange_source_operator.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/exchange/split_local_exchange.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_execution_params.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_builder_operators.h"
#include "exec/pipeline/sink/blackhole_table_sink_operator.h"
#include "exec/pipeline/sink/dictionary_cache_sink_operator.h"
#include "exec/pipeline/sink/export_sink_operator.h"
#include "exec/pipeline/sink/file_sink_operator.h"
#include "exec/pipeline/sink/memory_scratch_sink_operator.h"
#include "exec/pipeline/sink/mysql_table_sink_operator.h"
#include "exec/pipeline/sink/olap_table_sink_operator.h"
#include "exec/pipeline/sink/result_sink_operator.h"
#include "exec/runtime/query_context.h"
#include "exprs/expr_factory.h"
#include "fmt/format.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

#ifndef __APPLE__
#include "exec/data_sinks/iceberg_sink_builder.h"
#endif

namespace starrocks {

namespace {

using pipeline::OpFactories;
using pipeline::Operator;
using pipeline::PipelineBuilderContext;
using pipeline::UnifiedExecPlanFragmentParams;

std::string sink_type_name(TDataSinkType::type type) {
    auto it = _TDataSinkType_VALUES_TO_NAMES.find(type);
    return it == _TDataSinkType_VALUES_TO_NAMES.end() ? fmt::format("unknown data sink type {}", static_cast<int>(type))
                                                      : it->second;
}

bool is_final_sink(TDataSinkType::type type) {
    switch (type) {
    case TDataSinkType::RESULT_SINK:
    case TDataSinkType::OLAP_TABLE_SINK:
    case TDataSinkType::MULTI_OLAP_TABLE_SINK:
    case TDataSinkType::MEMORY_SCRATCH_SINK:
    case TDataSinkType::ICEBERG_TABLE_SINK:
    case TDataSinkType::HIVE_TABLE_SINK:
    case TDataSinkType::EXPORT_SINK:
    case TDataSinkType::BLACKHOLE_TABLE_SINK:
    case TDataSinkType::DICTIONARY_CACHE_SINK:
        return true;
    default:
        return false;
    }
}

StatusOr<std::unique_ptr<DataStreamSender>> create_data_stream_sender(
        RuntimeState* state, const TDataStreamSink& stream_sink, const RowDescriptor& row_desc,
        const TPlanFragmentExecParams& params, int32_t sender_id,
        const std::vector<TPlanFragmentDestination>& destinations) {
    const bool send_query_statistics_with_every_batch =
            params.__isset.send_query_statistics_with_every_batch && params.send_query_statistics_with_every_batch;
    const bool enable_exchange_pass_through =
            params.__isset.enable_exchange_pass_through && params.enable_exchange_pass_through;
    const bool enable_exchange_perf = params.__isset.enable_exchange_perf && params.enable_exchange_perf;

    auto sender = std::make_unique<DataStreamSender>(state, sender_id, row_desc, stream_sink, destinations,
                                                     send_query_statistics_with_every_batch,
                                                     enable_exchange_pass_through, enable_exchange_perf);
    TDataSink wrapper;
    wrapper.__set_type(TDataSinkType::DATA_STREAM_SINK);
    wrapper.__set_stream_sink(stream_sink);
    RETURN_IF_ERROR(sender->init(wrapper, state));
    return sender;
}

OperatorFactoryPtr create_exchange_sink_operator(PipelineBuilderContext* context, const TDataStreamSink& stream_sink,
                                                 const DataStreamSender& sender) {
    using namespace pipeline;
    auto* fragment_ctx = context->fragment_context();
    const bool is_dest_merge = stream_sink.__isset.is_merge && stream_sink.is_merge;

    bool is_pipeline_level_shuffle = false;
    int32_t dest_dop = 1;
    const bool enable_pipeline_level_shuffle = context->runtime_state()->query_ctx()->enable_pipeline_level_shuffle();
    if (enable_pipeline_level_shuffle &&
        (sender.get_partition_type() == TPartitionType::HASH_PARTITIONED ||
         sender.get_partition_type() == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED)) {
        is_pipeline_level_shuffle = true;
        dest_dop = stream_sink.dest_dop;
        DCHECK_GT(dest_dop, 0);
    }

    std::vector<TBucketProperty> bucket_properties;
    if (sender.get_partition_type() == TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED &&
        stream_sink.output_partition.__isset.bucket_properties) {
        bucket_properties = stream_sink.output_partition.bucket_properties;
    }

    auto sink_buffer = std::make_shared<SinkBuffer>(fragment_ctx, sender.destinations(), is_dest_merge);
    return std::make_shared<ExchangeSinkOperatorFactory>(
            context->next_operator_id(), stream_sink.dest_node_id, sink_buffer, sender.get_partition_type(),
            sender.destinations(), is_pipeline_level_shuffle, dest_dop, sender.sender_id(), sender.get_dest_node_id(),
            sender.get_partition_exprs(), !is_dest_merge && sender.get_enable_exchange_pass_through(),
            sender.get_enable_exchange_perf() && !context->has_aggregation, fragment_ctx, sender.output_columns(),
            bucket_properties);
}

Status build_result_sink(PipelineBuilderContext* context, OpFactories upstream, const TDataSink& thrift_sink,
                         const RowDescriptor& row_desc, const std::vector<TExpr>& output_exprs) {
    if (!thrift_sink.__isset.result_sink) {
        return Status::InternalError("Missing result sink");
    }
#ifndef __APPLE__
    if (thrift_sink.result_sink.__isset.type && thrift_sink.result_sink.type == TResultSinkType::FILE &&
        !thrift_sink.result_sink.__isset.file_options) {
        return Status::InternalError("Missing result file options");
    }
#endif

    auto* runtime_state = context->runtime_state();
    auto* fragment_ctx = context->fragment_context();
    const size_t dop = context->source_operator(upstream)->degree_of_parallelism();
    ResultSink result_sink(row_desc, output_exprs, thrift_sink.result_sink, 1024);

    if (runtime_state->query_options().__isset.enable_result_sink_accumulate &&
        runtime_state->query_options().enable_result_sink_accumulate) {
        pipeline::may_add_chunk_accumulate_operator(upstream, context, Operator::s_pseudo_plan_node_id_for_final_sink);
    }

    pipeline::OpFactoryPtr op;
    if (result_sink.get_sink_type() == TResultSinkType::FILE) {
        op = std::make_shared<pipeline::FileSinkOperatorFactory>(context->next_operator_id(),
                                                                 result_sink.get_output_exprs(),
                                                                 result_sink.get_file_opts(), dop, fragment_ctx);
    } else {
        op = std::make_shared<pipeline::ResultSinkOperatorFactory>(
                context->next_operator_id(), dop, result_sink.get_sink_type(), result_sink.isBinaryFormat(),
                result_sink.get_format_type(), result_sink.get_output_exprs(), fragment_ctx, result_sink.get_row_desc(),
                result_sink.get_output_column_names());
    }
    upstream.emplace_back(std::move(op));
    context->add_pipeline(upstream);
    return Status::OK();
}

Status build_stream_sink(PipelineBuilderContext* context, OpFactories upstream,
                         const UnifiedExecPlanFragmentParams& request, const RowDescriptor& row_desc,
                         const TDataSink& thrift_sink) {
    if (!thrift_sink.__isset.stream_sink) {
        return Status::InternalError("Missing data stream sink");
    }
    ASSIGN_OR_RETURN(auto sender, create_data_stream_sender(context->runtime_state(), thrift_sink.stream_sink, row_desc,
                                                            request.common().params, request.sender_id(),
                                                            request.common().params.destinations));
    upstream.emplace_back(create_exchange_sink_operator(context, thrift_sink.stream_sink, *sender));
    context->add_pipeline(upstream);
    return Status::OK();
}

Status build_multicast_sink(PipelineBuilderContext* context, OpFactories upstream,
                            const UnifiedExecPlanFragmentParams& request, const RowDescriptor& row_desc,
                            const TDataSink& thrift_sink) {
    using namespace pipeline;
    if (!thrift_sink.__isset.multi_cast_stream_sink) {
        return Status::InternalError("Missing multicast stream sink");
    }
    const auto& multicast = thrift_sink.multi_cast_stream_sink;
    if (multicast.sinks.size() != multicast.destinations.size()) {
        return Status::InternalError("Multicast stream sink and destination counts do not match");
    }

    auto* runtime_state = context->runtime_state();
    auto* upstream_source = context->source_operator(upstream);
    const auto upstream_plan_node_id = upstream.back()->plan_node_id();
    std::shared_ptr<MultiCastLocalExchanger> exchanger;
    if (runtime_state->enable_spill() && runtime_state->enable_multi_cast_local_exchange_spill()) {
        exchanger = std::make_shared<SpillableMultiCastLocalExchanger>(runtime_state, multicast.sinks.size(),
                                                                       upstream_plan_node_id);
    } else {
        exchanger = std::make_shared<InMemoryMultiCastLocalExchanger>(runtime_state, multicast.sinks.size());
    }

    upstream.emplace_back(std::make_shared<MultiCastLocalExchangeSinkOperatorFactory>(
            context->next_operator_id(), upstream_plan_node_id, exchanger));
    context->add_pipeline(upstream);

    for (size_t i = 0; i < multicast.sinks.size(); ++i) {
        ASSIGN_OR_RETURN(auto sender,
                         create_data_stream_sender(runtime_state, multicast.sinks[i], row_desc, request.common().params,
                                                   request.sender_id(), multicast.destinations[i]));
        OpFactories operators;
        auto source = std::make_shared<MultiCastLocalExchangeSourceOperatorFactory>(
                context->next_operator_id(), upstream_plan_node_id, i, exchanger);
        context->inherit_upstream_source_properties(source.get(), upstream_source);
        source->set_degree_of_parallelism(1);
        operators.emplace_back(std::move(source));
        if (multicast.sinks[i].__isset.limit && multicast.sinks[i].limit != -1) {
            operators.emplace_back(std::make_shared<LimitOperatorFactory>(
                    context->next_operator_id(), upstream_plan_node_id, multicast.sinks[i].limit, false));
        }
        operators.emplace_back(create_exchange_sink_operator(context, multicast.sinks[i], *sender));
        context->add_pipeline(operators);
    }
    return Status::OK();
}

Status build_split_stream_sink(PipelineBuilderContext* context, OpFactories upstream,
                               const UnifiedExecPlanFragmentParams& request, const RowDescriptor& row_desc,
                               const TDataSink& thrift_sink) {
    using namespace pipeline;
    if (!thrift_sink.__isset.split_stream_sink) {
        return Status::InternalError("Missing split stream sink");
    }
    const auto& split = thrift_sink.split_stream_sink;
    if (split.sinks.size() != split.destinations.size()) {
        return Status::InternalError("Split stream sink and destination counts do not match");
    }

    auto* runtime_state = context->runtime_state();
    std::vector<ExprContext*> split_expr_contexts;
    RETURN_IF_ERROR(ExprFactory::create_expr_trees(runtime_state->obj_pool(), split.splitExprs, &split_expr_contexts,
                                                   runtime_state));
    auto exchanger =
            std::make_shared<SplitLocalExchanger>(split.sinks.size(), split_expr_contexts, runtime_state->chunk_size());
    auto* upstream_source = context->source_operator(upstream);
    const auto upstream_plan_node_id = upstream.back()->plan_node_id();
    upstream.emplace_back(std::make_shared<MultiCastLocalExchangeSinkOperatorFactory>(
            context->next_operator_id(), upstream_plan_node_id, exchanger));
    context->add_pipeline(upstream);

    for (size_t i = 0; i < split.sinks.size(); ++i) {
        ASSIGN_OR_RETURN(auto sender,
                         create_data_stream_sender(runtime_state, split.sinks[i], row_desc, request.common().params,
                                                   request.sender_id(), split.destinations[i]));
        OpFactories operators;
        auto source = std::make_shared<MultiCastLocalExchangeSourceOperatorFactory>(
                context->next_operator_id(), upstream_plan_node_id, i, exchanger);
        context->inherit_upstream_source_properties(source.get(), upstream_source);
        operators.emplace_back(std::move(source));
        operators.emplace_back(create_exchange_sink_operator(context, split.sinks[i], *sender));
        context->add_pipeline(operators);
    }
    return Status::OK();
}

Status build_olap_sink(PipelineBuilderContext* context, OpFactories upstream,
                       const UnifiedExecPlanFragmentParams& request, const TDataSink& thrift_sink,
                       const std::vector<TExpr>& output_exprs) {
    using namespace pipeline;
    const bool is_multi = thrift_sink.type == TDataSinkType::MULTI_OLAP_TABLE_SINK;
    if ((!is_multi && !thrift_sink.__isset.olap_table_sink) ||
        (is_multi && !thrift_sink.__isset.multi_olap_table_sinks)) {
        return Status::InternalError(is_multi ? "Missing multi-OLAP table sinks" : "Missing OLAP table sink");
    }

    auto* runtime_state = context->runtime_state();
    const size_t desired_dop = request.pipeline_sink_dop();
    if (desired_dop == 0) {
        return Status::InternalError("OLAP table sink DOP must be positive");
    }
    runtime_state->set_num_per_fragment_instances(request.common().params.num_senders);

    std::vector<std::unique_ptr<AsyncDataSink>> sinks;
    sinks.reserve(desired_dop);
    for (size_t i = 0; i < desired_dop; ++i) {
        std::unique_ptr<AsyncDataSink> sink;
        if (is_multi) {
            sink = std::make_unique<MultiOlapTableSink>(runtime_state->obj_pool(), output_exprs);
        } else {
            Status status;
            sink = std::make_unique<OlapTableSink>(runtime_state->obj_pool(), output_exprs, &status, runtime_state);
            RETURN_IF_ERROR(status);
        }
        RETURN_IF_ERROR(sink->init(thrift_sink, runtime_state));
        sinks.emplace_back(std::move(sink));
    }

    auto sink_operator = std::make_shared<OlapTableSinkOperatorFactory>(
            context->next_operator_id(), context->fragment_context(), request.sender_id(), std::move(sinks));
    const size_t upstream_dop = context->source_operator(upstream)->degree_of_parallelism();
    if (desired_dop != upstream_dop) {
        auto operators = builder::maybe_interpolate_local_passthrough_exchange(
                context, runtime_state, Operator::s_pseudo_plan_node_id_for_final_sink, upstream, desired_dop);
        operators.emplace_back(std::move(sink_operator));
        context->add_pipeline(operators);
    } else {
        upstream.emplace_back(std::move(sink_operator));
        context->add_pipeline(upstream);
    }
    return Status::OK();
}

} // namespace

Status PipelineSinkBuilder::build(PipelineBuilderContext* context, OpFactories upstream,
                                  const UnifiedExecPlanFragmentParams& request, const RowDescriptor& row_desc) {
    const TDataSink& thrift_sink = request.output_sink();
    const auto& output_exprs = request.common().fragment.output_exprs;
    auto* runtime_state = context->runtime_state();
    auto* fragment_ctx = context->fragment_context();
    const size_t dop = context->source_operator(upstream)->degree_of_parallelism();

    if (is_final_sink(thrift_sink.type)) {
        runtime_state->query_ctx()->set_final_sink();
    }

    switch (thrift_sink.type) {
    case TDataSinkType::RESULT_SINK:
        return build_result_sink(context, std::move(upstream), thrift_sink, row_desc, output_exprs);
    case TDataSinkType::DATA_STREAM_SINK:
        return build_stream_sink(context, std::move(upstream), request, row_desc, thrift_sink);
    case TDataSinkType::MULTI_CAST_DATA_STREAM_SINK:
        return build_multicast_sink(context, std::move(upstream), request, row_desc, thrift_sink);
    case TDataSinkType::SPLIT_DATA_STREAM_SINK:
        return build_split_stream_sink(context, std::move(upstream), request, row_desc, thrift_sink);
    case TDataSinkType::OLAP_TABLE_SINK:
    case TDataSinkType::MULTI_OLAP_TABLE_SINK:
        return build_olap_sink(context, std::move(upstream), request, thrift_sink, output_exprs);
    case TDataSinkType::BLACKHOLE_TABLE_SINK:
        upstream.emplace_back(
                std::make_shared<pipeline::BlackHoleTableSinkOperatorFactory>(context->next_operator_id()));
        context->add_pipeline(upstream);
        return Status::OK();
    case TDataSinkType::EXPORT_SINK:
        if (!thrift_sink.__isset.export_sink) {
            return Status::InternalError("Missing export sink");
        }
        upstream.emplace_back(std::make_shared<pipeline::ExportSinkOperatorFactory>(
                context->next_operator_id(), thrift_sink.export_sink, output_exprs, dop, fragment_ctx));
        context->add_pipeline(upstream);
        return Status::OK();
    case TDataSinkType::MYSQL_TABLE_SINK:
        if (!thrift_sink.__isset.mysql_table_sink) {
            return Status::InternalError("Missing MySQL table sink");
        }
        upstream.emplace_back(std::make_shared<pipeline::MysqlTableSinkOperatorFactory>(
                context->next_operator_id(), thrift_sink.mysql_table_sink, output_exprs, dop, fragment_ctx));
        context->add_pipeline(upstream);
        return Status::OK();
    case TDataSinkType::MEMORY_SCRATCH_SINK:
        if (!thrift_sink.__isset.memory_scratch_sink) {
            return Status::InternalError("Missing memory scratch sink");
        }
        if (dop != 1) {
            return Status::InternalError("Memory scratch sink requires DOP 1");
        }
        upstream.emplace_back(std::make_shared<pipeline::MemoryScratchSinkOperatorFactory>(
                context->next_operator_id(), row_desc, output_exprs, fragment_ctx));
        context->add_pipeline(upstream);
        return Status::OK();
#ifndef __APPLE__
    case TDataSinkType::ICEBERG_TABLE_SINK:
    case TDataSinkType::ICEBERG_DELETE_SINK:
    case TDataSinkType::ICEBERG_ROW_DELTA_SINK:
        return IcebergSinkBuilder(output_exprs).build(std::move(upstream), thrift_sink, context);
#else
    case TDataSinkType::ICEBERG_TABLE_SINK:
    case TDataSinkType::ICEBERG_DELETE_SINK:
    case TDataSinkType::ICEBERG_ROW_DELTA_SINK:
        return Status::NotSupported("Iceberg table sink is disabled on macOS");
#endif
    case TDataSinkType::HIVE_TABLE_SINK:
        return HiveSinkBuilder(output_exprs).build(std::move(upstream), thrift_sink, context);
    case TDataSinkType::TABLE_FUNCTION_TABLE_SINK:
        return TableFunctionSinkBuilder(output_exprs).build(std::move(upstream), thrift_sink, context);
    case TDataSinkType::DICTIONARY_CACHE_SINK:
        if (!thrift_sink.__isset.dictionary_cache_sink) {
            return Status::InternalError("Missing dictionary cache sink");
        }
        upstream.emplace_back(std::make_shared<pipeline::DictionaryCacheSinkOperatorFactory>(
                context->next_operator_id(), thrift_sink.dictionary_cache_sink, fragment_ctx));
        context->add_pipeline(upstream);
        return Status::OK();
    case TDataSinkType::NOOP_SINK:
        upstream.emplace_back(std::make_shared<pipeline::NoopSinkOperatorFactory>(context->next_operator_id(),
                                                                                  upstream.back()->plan_node_id()));
        context->add_pipeline(upstream);
        return Status::OK();
    case TDataSinkType::SCHEMA_TABLE_SINK:
        return Status::NotSupported("SCHEMA_TABLE_SINK is not supported by the pipeline engine");
    case TDataSinkType::DATA_SPLIT_SINK:
        return Status::NotSupported("DATA_SPLIT_SINK is not implemented");
    default:
        return Status::InternalError(fmt::format("{} is not implemented", sink_type_name(thrift_sink.type)));
    }
}

} // namespace starrocks
