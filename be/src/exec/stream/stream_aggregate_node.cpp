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

#include "exec/stream/stream_aggregate_node.h"

#include "exec/stream/aggregate/stream_aggregate_sink_operator.h"
#include "exec/stream/aggregate/stream_aggregate_source_operator.h"

namespace starrocks {

using StreamAggregatorPtr = std::shared_ptr<stream::StreamAggregator>;
using StreamAggregatorFactory = AggregatorFactoryBase<stream::StreamAggregator>;

Status StreamAggregateNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.stream_agg_node.grouping_exprs, &_group_by_expr_ctxs, state));
    for (auto& expr : _group_by_expr_ctxs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_groupby()) {
            return Status::NotSupported(fmt::format("group by type {} is not supported", type_desc.debug_string()));
        }
    }

    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory> > StreamAggregateNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);

    auto try_interpolate_local_shuffle = [this, context](auto& ops) {
        return context->maybe_interpolate_local_shuffle_exchange(runtime_state(), ops, [this]() {
            std::vector<ExprContext*> group_by_expr_ctxs;
            Expr::create_expr_trees(_pool, _tnode.stream_agg_node.grouping_exprs, &group_by_expr_ctxs, runtime_state());
            return group_by_expr_ctxs;
        });
    };
    bool could_local_shuffle = context->could_local_shuffle(ops_with_sink);
    if (could_local_shuffle) {
        ops_with_sink = try_interpolate_local_shuffle(ops_with_sink);
    }

    auto aggregator_factory = std::make_shared<stream::StreamAggregatorFactory>(_tnode);
    aggregator_factory->set_aggr_mode(AM_DEFAULT);

    auto agg_sink_operator = std::make_shared<stream::StreamAggregateSinkOperatorFactory>(context->next_operator_id(),
                                                                                          id(), aggregator_factory);
    ops_with_sink.push_back(std::move(agg_sink_operator));
    context->add_pipeline(ops_with_sink);

    // shared by sink operator and source operator
    OpFactories operators_with_source;
    auto source_operator = std::make_shared<stream::StreamAggregateSourceOperatorFactory>(context->next_operator_id(),
                                                                                          id(), aggregator_factory);
    // Aggregator must be used by a pair of sink and source operators,
    // so ops_with_source's degree of parallelism must be equal with operators_with_sink's
    auto* upstream_source_op = context->source_operator(ops_with_sink);
    source_operator->set_degree_of_parallelism(upstream_source_op->degree_of_parallelism());
    operators_with_source.push_back(std::move(source_operator));

    return operators_with_source;
}

} // namespace starrocks
