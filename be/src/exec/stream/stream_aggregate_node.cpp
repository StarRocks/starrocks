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

namespace starrocks {

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
    OpFactories operators_with_sink = _children[0]->decompose_to_pipeline(context);
    // We cannot get degree of parallelism from PipelineBuilderContext, of which is only a suggest value
    // and we may set other parallelism for source operator in many special cases
    size_t degree_of_parallelism =
            down_cast<pipeline::SourceOperatorFactory*>(operators_with_sink[0].get())->degree_of_parallelism();

    // shared by sink operator and source operator
    OpFactories operators_with_source;
    auto aggregator_factory = std::make_shared<stream::StreamAggregatorFactory>(_tnode);
    auto source_operator = std::make_shared<stream::StreamAggregateOperatorFactory>(context->next_operator_id(), id(),
                                                                                    aggregator_factory);
    // Aggregator must be used by a pair of sink and source operators,
    // so operators_with_source's degree of parallelism must be equal with operators_with_sink's
    source_operator->set_degree_of_parallelism(degree_of_parallelism);
    operators_with_source.push_back(std::move(source_operator));
    return operators_with_source;
}

} // namespace starrocks
