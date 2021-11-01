// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_builder.h"

#include "exec/exec_node.h"

namespace starrocks::pipeline {

OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(OpFactories& pred_operators) {
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

OpFactories PipelineBuilderContext::maybe_interpolate_local_shuffle_exchange(
        OpFactories& pred_operators, const std::vector<ExprContext*>& partition_expr_ctxs) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());

    // If DOP is one, we needn't partition input chunks.
    size_t shuffle_partitions_num = degree_of_parallelism();
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    // TODO: make max_row_count as config::vector_chunk_size * shuffle_partitions_num when the local shuffle is lock free.
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(config::vector_chunk_size);
    auto local_shuffle_source = std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), mem_mgr);
    auto local_shuffle =
            std::make_shared<PartitionExchanger>(mem_mgr, local_shuffle_source.get(), true, partition_expr_ctxs);

    // Append local shuffle sink to the tail of the current pipeline, which comes to end.
    auto local_shuffle_sink = std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), local_shuffle);
    pred_operators.emplace_back(std::move(local_shuffle_sink));
    add_pipeline(pred_operators);

    // Create a new pipeline beginning with a local shuffle source.
    OpFactories operators_source_with_local_shuffle;
    local_shuffle_source->set_degree_of_parallelism(shuffle_partitions_num);
    operators_source_with_local_shuffle.emplace_back(std::move(local_shuffle_source));

    return operators_source_with_local_shuffle;
}

OpFactories PipelineBuilderContext::gather_pipelines_to_one(std::vector<OpFactories>& pred_operators_list) {
    if (pred_operators_list.size() == 1) {
        return maybe_interpolate_local_passthrough_exchange(pred_operators_list[0]);
    }

    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(config::vector_chunk_size);
    auto local_exchange_source = std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), mem_mgr);
    auto exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());

    // Append a local exchange sink to the tail of each pipeline, which comes to end.
    for (auto& pred_operators : pred_operators_list) {
        auto local_exchange_sink = std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), exchanger);
        pred_operators.emplace_back(std::move(local_exchange_sink));
        add_pipeline(pred_operators);
    }

    // Create a new pipeline beginning with a local exchange source.
    OpFactories operators_source_with_local_exchange;
    local_exchange_source->set_degree_of_parallelism(1);
    operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));

    return operators_source_with_local_exchange;
}

Pipelines PipelineBuilder::build(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    _context.add_pipeline(operators);
    return _context.get_pipelines();
}

} // namespace starrocks::pipeline