// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/stream/stream_agg.h"

#include "exec/stream/aggregate/streaming_aggregate_sink_operator.h"
#include "exec/stream/aggregate/streaming_aggregate_source_operator.h"

namespace starrocks {

Status StreamAggNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.stream_agg_node.grouping_exprs, &_group_by_expr_ctxs));
    for (auto& expr : _group_by_expr_ctxs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_groupby()) {
            return Status::NotSupported(fmt::format("group by type {} is not supported", type_desc.debug_string()));
        }
    }

    if (tnode.stream_agg_node.__isset.detail_imt) {
        auto& detail_imt = tnode.stream_agg_node.detail_imt;
        VLOG(2) << "detail_imt: " << detail_imt;
        if (detail_imt.imt_type != TIMTType::OLAP_TABLE) {
            return Status::NotSupported("only OLAP_TABLE imt is supported");
        }

        // TODO: use RouteInfo to lookup table
        _imt_detail = std::make_shared<IMTStateTable>(detail_imt);
        RETURN_IF_ERROR(_imt_detail->init());
        VLOG(2) << "_imt_detail: " << _imt_detail->debug_string();
    }
    if (tnode.stream_agg_node.__isset.agg_result_imt) {
        auto& agg_result_imt = tnode.stream_agg_node.agg_result_imt;
        VLOG(2) << "agg_result_imt: " << agg_result_imt;
        if (agg_result_imt.imt_type != TIMTType::OLAP_TABLE) {
            return Status::NotSupported("only OLAP_TABLE imt is supported");
        }

        // TODO: use RouteInfo to lookup table
        _imt_agg_result = std::make_shared<IMTStateTable>(agg_result_imt);
        RETURN_IF_ERROR(_imt_agg_result->init());
        VLOG(2) << "_agg_result_imt: " << _imt_agg_result->debug_string();
    }

    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory> > StreamAggNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators_with_sink = _children[0]->decompose_to_pipeline(context);
    // We cannot get degree of parallelism from PipelineBuilderContext, of which is only a suggest value
    // and we may set other parallelism for source operator in many special cases
    size_t degree_of_parallelism =
            down_cast<SourceOperatorFactory*>(operators_with_sink[0].get())->degree_of_parallelism();

    // shared by sink operator factory and source operator factory
    AggregatorFactoryPtr aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);

    // Create a shared RefCountedRuntimeFilterCollector
    auto sink_operator = std::make_shared<pipeline::StreamingAggregateSinkOperatorFactory>(context->next_operator_id(),
                                                                                           id(), aggregator_factory,
                                                                                           _imt_detail, _imt_agg_result);
    // Initialize OperatorFactory's fields involving runtime filters.
    operators_with_sink.emplace_back(sink_operator);
    context->add_pipeline(operators_with_sink);

    OpFactories operators_with_source;
    FragmentContext* fragment_context = context->fragment_context();
    auto source_operator = std::make_shared<pipeline::StreamingAggregateSourceOperatorFactory>(
            context->next_operator_id(), id(), aggregator_factory,
            _imt_detail, _imt_agg_result,
            fragment_context);
    // Aggregator must be used by a pair of sink and source operators,
    // so operators_with_source's degree of parallelism must be equal with operators_with_sink's
    source_operator->set_degree_of_parallelism(degree_of_parallelism);
    operators_with_source.push_back(std::move(source_operator));
    return operators_with_source;
}
} // namespace starrocks