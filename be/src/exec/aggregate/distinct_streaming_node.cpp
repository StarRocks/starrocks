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

#include "exec/aggregate/distinct_streaming_node.h"

#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_streaming_source_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {

StatusOr<pipeline::OpFactories> DistinctStreamingNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    ASSIGN_OR_RETURN(auto ops_with_sink, _children[0]->decompose_to_pipeline(context));
    size_t degree_of_parallelism = context->source_operator(ops_with_sink)->degree_of_parallelism();
    auto should_cache = context->should_interpolate_cache_operator(id(), ops_with_sink[0]);
    auto* upstream_source_op = context->source_operator(ops_with_sink);

    bool could_local_shuffle = !should_cache && !context->enable_group_execution();
    if (could_local_shuffle && _tnode.agg_node.__isset.interpolate_passthrough &&
        _tnode.agg_node.interpolate_passthrough && context->could_local_shuffle(ops_with_sink)) {
        ops_with_sink = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), ops_with_sink,
                                                                              degree_of_parallelism, true);
    }

    auto operators_generator = [this, should_cache, upstream_source_op, context](bool post_cache) {
        // shared by sink operator factory and source operator factory
        AggregatorFactoryPtr aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);
        AggrMode aggr_mode =
                should_cache ? (post_cache ? AM_STREAMING_POST_CACHE : AM_STREAMING_PRE_CACHE) : AM_DEFAULT;
        aggregator_factory->set_aggr_mode(aggr_mode);
        auto sink_operator = std::make_shared<AggregateDistinctStreamingSinkOperatorFactory>(
                context->next_operator_id(), id(), aggregator_factory);
        auto source_operator = std::make_shared<AggregateDistinctStreamingSourceOperatorFactory>(
                context->next_operator_id(), id(), aggregator_factory);
        context->inherit_upstream_source_properties(source_operator.get(), upstream_source_op);
        return std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>(sink_operator, source_operator);
    };
    auto [agg_sink_op, agg_source_op] = operators_generator(true);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, agg_sink_op.get(), context, rc_rf_probe_collector);
    ops_with_sink.emplace_back(std::move(agg_sink_op));

    OpFactories ops_with_source;
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, agg_source_op.get(), context, rc_rf_probe_collector);
    ops_with_source.push_back(std::move(agg_source_op));

    if (should_cache) {
        ops_with_source =
                context->interpolate_cache_operator(id(), ops_with_sink, ops_with_source, operators_generator);
    }
    context->add_pipeline(ops_with_sink);
    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    ops_with_source = context->maybe_interpolate_debug_ops(runtime_state(), _id, ops_with_source);
    return ops_with_source;
}

} // namespace starrocks
