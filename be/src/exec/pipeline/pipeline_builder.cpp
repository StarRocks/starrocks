// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_builder.h"

#include "exec/exec_node.h"

namespace starrocks::pipeline {

OpFactories PipelineBuilderContext::maybe_interpolate_local_exchange(OpFactories& pred_operators) {
    // predecessor pipeline has multiple drivers that will produce multiple output streams, but sort operator is
    // not parallelized now and can not accept multiple streams as input, so add a LocalExchange to gather multiple
    // streams and produce one output stream piping into the sort operator.
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());
    auto* source_operator = down_cast<SourceOperatorFactory*>(pred_operators[0].get());
    if (source_operator->degree_of_parallelism() > 1) {
        auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(config::vector_chunk_size);
        auto local_exchange_source = std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), mem_mgr);
        auto local_exchange = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());
        auto local_exchange_sink =
                std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), local_exchange);
        // Add LocalExchangeSinkOperator to predecessor pipeline.
        pred_operators.emplace_back(std::move(local_exchange_sink));
        // predecessor pipeline comes to end.
        add_pipeline(pred_operators);

        OpFactories operators_source_with_local_exchange;
        // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.
        local_exchange_source->set_degree_of_parallelism(1);
        // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
        operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
        return operators_source_with_local_exchange;
    } else {
        return pred_operators;
    }
}

Pipelines PipelineBuilder::build(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    _context.add_pipeline(operators);
    return _context.get_pipelines();
}

} // namespace starrocks::pipeline