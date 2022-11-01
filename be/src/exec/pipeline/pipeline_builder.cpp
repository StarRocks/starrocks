// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/pipeline_builder.h"

#include "exec/exec_node.h"
#include "exec/query_cache/cache_manager.h"
#include "exec/query_cache/cache_operator.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exec/query_cache/lane_arbiter.h"
#include "exec/query_cache/multilane_operator.h"

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
    auto* source_operator = down_cast<SourceOperatorFactory*>(pred_operators[0].get());
    if (!force && source_operator->degree_of_parallelism() == num_receivers) {
        return pred_operators;
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    int buffer_size = std::max(num_receivers, static_cast<int>(source_operator->degree_of_parallelism())) *
                      kLocalExchangeBufferChunks;
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(state->chunk_size() * buffer_size);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
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
        RuntimeState* state, OpFactories& pred_operators, const std::vector<ExprContext*>& partition_expr_ctxs,
        const TPartitionType::type part_type) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());

    // If DOP is one, we needn't partition input chunks.
    size_t shuffle_partitions_num = degree_of_parallelism();
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    auto* pred_source_op = down_cast<SourceOperatorFactory*>(pred_operators[0].get());

    // To make sure at least one partition source operator is ready to output chunk before sink operators are full.
    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(shuffle_partitions_num * state->chunk_size() *
                                                                kLocalExchangeBufferChunks);
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_shuffle_source->set_runtime_state(state);
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
        auto* source_operator = down_cast<SourceOperatorFactory*>(pred_ops[0].get());
        max_row_count += source_operator->degree_of_parallelism() * state->chunk_size();
    }

    auto pseudo_plan_node_id = next_pseudo_plan_node_id();
    auto mem_mgr = std::make_shared<LocalExchangeMemoryManager>(max_row_count * kLocalExchangeBufferChunks);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
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

size_t PipelineBuilderContext::dop_of_source_operator(int source_node_id) {
    auto* morsel_queue_factory = morsel_queue_factory_of_source_operator(source_node_id);
    return morsel_queue_factory->size();
}

MorselQueueFactory* PipelineBuilderContext::morsel_queue_factory_of_source_operator(int source_node_id) {
    auto& morsel_queues = _fragment_context->morsel_queue_factories();
    DCHECK(morsel_queues.count(source_node_id));
    return morsel_queues[source_node_id].get();
}

MorselQueueFactory* PipelineBuilderContext::morsel_queue_factory_of_source_operator(
        const SourceOperatorFactory* source_op) {
    if (!source_op->with_morsels()) {
        return nullptr;
    }

    return morsel_queue_factory_of_source_operator(source_op->plan_node_id());
}

bool PipelineBuilderContext::need_local_shuffle(OpFactories ops) const {
    return down_cast<SourceOperatorFactory*>(ops[0].get())->need_local_shuffle();
}

bool PipelineBuilderContext::should_interpolate_cache_operator(OpFactoryPtr& source_op, int32_t plan_node_id) {
    if (!_fragment_context->enable_cache()) {
        return false;
    }
    const auto& cache_param = _fragment_context->cache_param();
    if (cache_param.plan_node_id != plan_node_id) {
        return false;
    }
    return dynamic_cast<pipeline::OlapScanOperatorFactory*>(source_op.get()) != nullptr;
}

OpFactories PipelineBuilderContext::interpolate_cache_operator(
        OpFactories& upstream_pipeline, OpFactories& downstream_pipeline,
        const std::function<std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>(bool)>& merge_operators_generator) {
    DCHECK(should_interpolate_cache_operator(upstream_pipeline[0], downstream_pipeline[0]->plan_node_id()));

    const auto& cache_param = _fragment_context->cache_param();

    auto sink_op = upstream_pipeline.back();
    upstream_pipeline.pop_back();
    auto source_op = downstream_pipeline.front();
    downstream_pipeline.clear();

    auto dop = down_cast<SourceOperatorFactory*>(source_op.get())->degree_of_parallelism();
    auto conjugate_op = std::make_shared<query_cache::ConjugateOperatorFactory>(sink_op, source_op);
    upstream_pipeline.push_back(std::move(conjugate_op));

    auto last_ml_op_idx = upstream_pipeline.size() - 1;
    for (auto i = 1; i < upstream_pipeline.size(); ++i) {
        auto& op = upstream_pipeline[i];
        auto ml_op =
                std::make_shared<query_cache::MultilaneOperatorFactory>(next_operator_id(), op, cache_param.num_lanes);
        // only last multilane operator in the pipeline driver can work in passthrough mode
        auto can_passthrough = i == last_ml_op_idx;
        ml_op->set_can_passthrough(can_passthrough);
        upstream_pipeline[i] = std::move(ml_op);
    }

    auto cache_mgr = ExecEnv::GetInstance()->cache_mgr();
    auto cache_op = std::make_shared<query_cache::CacheOperatorFactory>(next_operator_id(), next_pseudo_plan_node_id(),
                                                                        cache_mgr, cache_param);
    upstream_pipeline.push_back(cache_op);

    auto merge_operators = merge_operators_generator(true);
    upstream_pipeline.push_back(std::move(std::get<0>(merge_operators)));
    downstream_pipeline.push_back(std::move(std::get<1>(merge_operators)));
    down_cast<SourceOperatorFactory*>(downstream_pipeline.front().get())->set_degree_of_parallelism(dop);
    return downstream_pipeline;
}

Pipelines PipelineBuilder::build(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    _context.add_pipeline(operators);
    return _context.get_pipelines();
}

} // namespace starrocks::pipeline
