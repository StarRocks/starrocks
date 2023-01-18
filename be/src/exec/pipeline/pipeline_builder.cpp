// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/pipeline_builder.h"

#include "exec/exec_node.h"

namespace starrocks::pipeline {

OpFactories PipelineBuilderContext::maybe_interpolate_local_broadcast_exchange(RuntimeState* state,
                                                                               OpFactories& pred_operators,
                                                                               int num_receivers) {
    if (num_receivers == 1) {
        return maybe_interpolate_local_passthrough_exchange(state, pred_operators);
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(state->chunk_size() * num_receivers *
                                                                kLocalExchangeBufferChunks);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    local_exchange_source->set_could_local_shuffle(true);
    local_exchange_source->set_partition_type(source_operator(pred_operators)->partition_type());
    auto local_exchange = std::make_shared<BroadcastExchanger>(mem_mgr, local_exchange_source.get());
    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, local_exchange);
    // Add LocalExchangeSinkOperator to predecessor pipeline.
    pred_operators.emplace_back(std::move(local_exchange_sink));
    // predecessor pipeline comes to end.
    add_pipeline(pred_operators);

    OpFactories operators_source_with_local_exchange;
    // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.
    local_exchange_source->set_degree_of_parallelism(num_receivers);
    // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
    operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
    return operators_source_with_local_exchange;
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(RuntimeState* state,
                                                                                 OpFactories& pred_operators) {
    return maybe_interpolate_local_passthrough_exchange(state, pred_operators, 1);
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(RuntimeState* state,
                                                                                 OpFactories& pred_operators,
                                                                                 int num_receivers, bool force) {
    // predecessor pipeline has multiple drivers that will produce multiple output streams, but sort operator is
    // not parallelized now and can not accept multiple streams as input, so add a LocalExchange to gather multiple
    // streams and produce one output stream piping into the sort operator.
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());
    auto* source_op = source_operator(pred_operators);
    if (!force && source_op->degree_of_parallelism() == num_receivers) {
        return pred_operators;
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    int buffer_size =
            std::max(num_receivers, static_cast<int>(source_op->degree_of_parallelism())) * kLocalExchangeBufferChunks;
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(state->chunk_size() * buffer_size);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    local_exchange_source->set_could_local_shuffle(true);
    local_exchange_source->set_partition_type(source_op->partition_type());
    auto local_exchange = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());
    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, local_exchange);
    // Add LocalExchangeSinkOperator to predecessor pipeline.
    pred_operators.emplace_back(std::move(local_exchange_sink));
    // predecessor pipeline comes to end.
    add_pipeline(pred_operators);

    OpFactories operators_source_with_local_exchange;
    // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.
    local_exchange_source->set_degree_of_parallelism(num_receivers);
    // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
    operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
    return operators_source_with_local_exchange;
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_shuffle_exchange(
        RuntimeState* state, OpFactories& pred_operators, const std::vector<ExprContext*>& self_partition_exprs) {
    return maybe_interpolate_local_shuffle_exchange(state, pred_operators,
                                                    [&self_partition_exprs]() { return self_partition_exprs; });
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_shuffle_exchange(
        RuntimeState* state, OpFactories& pred_operators,
        const PartitionExprsGenerator& self_partition_exprs_generator) {
    auto* source_op = source_operator(pred_operators);
    if (!source_op->could_local_shuffle()) {
        return pred_operators;
    }

    if (!source_op->partition_exprs().empty()) {
        return _do_maybe_interpolate_local_shuffle_exchange(state, pred_operators, source_op->partition_exprs(),
                                                            source_op->partition_type());
    }

    return _do_maybe_interpolate_local_shuffle_exchange(state, pred_operators, self_partition_exprs_generator(),
                                                        source_op->partition_type());
}

OpFactories PipelineBuilderContext::_do_maybe_interpolate_local_shuffle_exchange(
        RuntimeState* state, OpFactories& pred_operators, const std::vector<ExprContext*>& partition_expr_ctxs,
        const TPartitionType::type part_type) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());

    // If DOP is one, we needn't partition input chunks.
    size_t shuffle_partitions_num = degree_of_parallelism();
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    auto* pred_source_op = source_operator(pred_operators);

    // To make sure at least one partition source operator is ready to output chunk before sink operators are full.
    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(shuffle_partitions_num * state->chunk_size() *
                                                                kLocalExchangeBufferChunks);
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_shuffle_source->set_runtime_state(state);
    local_shuffle_source->set_could_local_shuffle(pred_source_op->partition_exprs().empty());
    local_shuffle_source->set_partition_type(pred_source_op->partition_type());

    auto local_shuffle =
            std::make_shared<PartitionExchanger>(mem_mgr, local_shuffle_source.get(), part_type, partition_expr_ctxs,
                                                 pred_source_op->degree_of_parallelism());

    // Append local shuffle sink to the tail of the current pipeline, which comes to end.
    auto local_shuffle_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, local_shuffle);
    pred_operators.emplace_back(std::move(local_shuffle_sink));
    add_pipeline(pred_operators);

    // Create a new pipeline beginning with a local shuffle source.
    OpFactories operators_source_with_local_shuffle;
    local_shuffle_source->set_degree_of_parallelism(shuffle_partitions_num);
    operators_source_with_local_shuffle.emplace_back(std::move(local_shuffle_source));

    return operators_source_with_local_shuffle;
}

OpFactories PipelineBuilderContext::maybe_gather_pipelines_to_one(RuntimeState* state,
                                                                  std::vector<OpFactories>& pred_operators_list) {
    // If there is only one pred pipeline, we needn't local passthrough anymore.
    if (pred_operators_list.size() == 1) {
        return pred_operators_list[0];
    }

    // Approximately, each pred driver can output state->chunk_size() rows at the same time.
    size_t max_row_count = 0;
    for (const auto& pred_ops : pred_operators_list) {
        auto* source_op = source_operator(pred_ops);
        max_row_count += source_op->degree_of_parallelism() * state->chunk_size();
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(max_row_count * kLocalExchangeBufferChunks);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    local_exchange_source->set_could_local_shuffle(true);

    auto exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());

    // Append a local exchange sink to the tail of each pipeline, which comes to end.
    for (auto& pred_operators : pred_operators_list) {
        auto local_exchange_sink =
                std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), pseudo_plan_node_id, exchanger);
        pred_operators.emplace_back(std::move(local_exchange_sink));
        add_pipeline(pred_operators);
    }

    // Create a new pipeline beginning with a local exchange source.
    OpFactories operators_source_with_local_exchange;
    local_exchange_source->set_degree_of_parallelism(degree_of_parallelism());
    operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));

    return operators_source_with_local_exchange;
}

MorselQueue* PipelineBuilderContext::morsel_queue_of_source_operator(const SourceOperatorFactory* source_op) {
    if (!source_op->with_morsels()) {
        return nullptr;
    }
    auto& morsel_queues = _fragment_context->morsel_queues();
    auto source_id = source_op->plan_node_id();
    DCHECK(morsel_queues.count(source_id));
    return morsel_queues[source_id].get();
}

size_t PipelineBuilderContext::degree_of_parallelism_of_source_operator(int32_t source_node_id) const {
    auto& morsel_queues = _fragment_context->morsel_queues();
    auto it = morsel_queues.find(source_node_id);
    if (it == morsel_queues.end()) {
        return _degree_of_parallelism;
    }

    // The degree_of_parallelism of the SourceOperator with morsel is not more than the number of morsels
    // If table is empty, then morsel size is zero and we still set degree of parallelism to 1
    return std::min<size_t>(std::max<size_t>(1, it->second->max_degree_of_parallelism()), _degree_of_parallelism);
}

size_t PipelineBuilderContext::degree_of_parallelism_of_source_operator(const SourceOperatorFactory* source_op) const {
    return degree_of_parallelism_of_source_operator(source_op->plan_node_id());
}

SourceOperatorFactory* PipelineBuilderContext::source_operator(OpFactories ops) {
    return down_cast<SourceOperatorFactory*>(ops[0].get());
}

bool PipelineBuilderContext::could_local_shuffle(OpFactories ops) const {
    return down_cast<SourceOperatorFactory*>(ops[0].get())->could_local_shuffle();
}

Pipelines PipelineBuilder::build(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    _context.add_pipeline(operators);
    return _context.get_pipelines();
}

} // namespace starrocks::pipeline
