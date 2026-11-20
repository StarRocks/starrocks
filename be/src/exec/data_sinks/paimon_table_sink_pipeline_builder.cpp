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

#include "exec/data_sinks/paimon_table_sink_pipeline_builder.h"

#include <algorithm>

#include "common/config_exec_flow_fwd.h"
#include "connector/hive/paimon/paimon_chunk_sink.h"
#include "data_sink/external/paimon_table_sink.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_builder_operators.h"
#include "exec/pipeline/sink/connector_sink_operator.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks {

Status decompose_paimon_table_sink_to_pipeline(const PaimonTableSink& sink, pipeline::OpFactories prev_operators,
                                               const TDataSink& thrift_sink,
                                               pipeline::PipelineBuilderContext* context) {
    if (!thrift_sink.__isset.paimon_table_sink) return Status::InvalidArgument("Missing Paimon table sink");
    const auto& target = thrift_sink.paimon_table_sink;
    auto* state = context->runtime_state();
    auto* fragment = context->fragment_context();
    if (fragment->enable_adaptive_dop()) {
        return Status::NotSupported(
                "Paimon writes require fixed sink DOP for producer completeness and bucket ownership");
    }
    const auto dop = context->data_sink_dop();
    if (dop == 0) return Status::InvalidArgument("Paimon sink DOP must be positive");
    auto sink_context = std::make_shared<connector::PaimonChunkSinkContext>();
    sink_context->sink = target;
    sink_context->output_exprs = sink.get_output_expr();
    sink_context->runtime_state = state;
    auto* services = state->query_execution_services();
    if (services == nullptr || services->execution == nullptr) {
        return Status::InternalError("Paimon sink requires injected query execution services");
    }
    sink_context->executor = services->execution->pipeline_sink_io_pool;
    auto provider = std::make_unique<connector::PaimonChunkSinkProvider>(sink_context);
    auto sink_operator = std::make_shared<pipeline::ConnectorSinkOperatorFactory>(context->next_operator_id(),
                                                                                  std::move(provider), fragment);
    const auto node_id = pipeline::Operator::s_pseudo_plan_node_id_for_final_sink;
    if (target.bucket_count > 0) {
        if (!target.__isset.bucket_routed || !target.bucket_routed || !target.__isset.routing_bucket_slot_id) {
            return Status::NotSupported(
                    "Paimon fixed-bucket sink requires native bucket routing before the global exchange");
        }
        if (sink_context->output_exprs.size() != target.column_names.size()) {
            return Status::InvalidArgument("Paimon sink needs full-schema output expressions");
        }
        std::vector<TExpr> partition_exprs;
        for (const auto& partition : target.partition_columns) {
            auto it = std::find(target.column_names.begin(), target.column_names.end(), partition);
            if (it == target.column_names.end()) return Status::InvalidArgument("Unknown Paimon partition column");
            partition_exprs.push_back(sink_context->output_exprs[it - target.column_names.begin()]);
        }
        std::vector<ExprContext*> keys;
        RETURN_IF_ERROR(ExprFactory::create_expr_trees(state->obj_pool(), partition_exprs, &keys, state));
        keys.push_back(state->obj_pool()->add(new ExprContext(
                state->obj_pool()->add(new ColumnRef(TypeDescriptor(TYPE_INT), target.routing_bucket_slot_id)))));
        // Force a fixed hash exchange on (native partition values, native bucket id) even when
        // the upstream source reports already-shuffled. Never use writer scaling/key-adaptive
        // exchange: either can create two native writers for one PK partition/bucket.
        prev_operators = pipeline::builder::maybe_interpolate_grouped_exchange(context, node_id, prev_operators);
        auto memory = std::make_shared<pipeline::ChunkBufferMemoryManager>(
                dop, config::local_exchange_buffer_mem_limit_per_driver);
        auto source = std::make_shared<pipeline::LocalExchangeSourceOperatorFactory>(context->next_operator_id(),
                                                                                     node_id, memory);
        source->set_runtime_state(state);
        context->inherit_upstream_source_properties(source.get(), context->source_operator(prev_operators));
        source->set_could_local_shuffle(false);
        source->set_degree_of_parallelism(dop);
        auto exchange = std::make_shared<pipeline::PartitionExchanger>(
                memory, source.get(), TPartitionType::HASH_PARTITIONED, keys, std::vector<TBucketProperty>{});
        prev_operators.emplace_back(std::make_shared<pipeline::LocalExchangeSinkOperatorFactory>(
                context->next_operator_id(), node_id, exchange));
        context->add_pipeline(prev_operators);
        pipeline::OpFactories operators{source, sink_operator};
        context->add_pipeline(operators);
    } else {
        // Force the requested number of sink drivers even for empty/VALUES sources. Every
        // driver must report a terminal producer marker; scaling would invalidate FE's set.
        auto operators = pipeline::builder::maybe_interpolate_local_passthrough_exchange(context, state, node_id,
                                                                                         prev_operators, dop, true);
        operators.emplace_back(std::move(sink_operator));
        context->add_pipeline(operators);
    }
    return Status::OK();
}

} // namespace starrocks
