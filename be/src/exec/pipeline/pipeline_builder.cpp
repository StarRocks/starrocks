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

#include "exec/pipeline/pipeline_builder.h"

#include "common/config.h"
#include "exec/exec_node.h"
#include "exec/pipeline/adaptive/collect_stats_context.h"
#include "exec/pipeline/adaptive/collect_stats_sink_operator.h"
#include "exec/pipeline/adaptive/collect_stats_source_operator.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/spill_process_operator.h"
#include "exec/query_cache/cache_manager.h"
#include "exec/query_cache/cache_operator.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exec/query_cache/lane_arbiter.h"
#include "exec/query_cache/multilane_operator.h"

namespace starrocks::pipeline {

/// PipelineBuilderContext.
OpFactories PipelineBuilderContext::maybe_interpolate_local_broadcast_exchange(RuntimeState* state,
                                                                               int32_t plan_node_id,
                                                                               OpFactories& pred_operators,
                                                                               int num_receivers) {
    if (num_receivers == 1) {
        return maybe_interpolate_local_passthrough_exchange(state, plan_node_id, pred_operators);
    }

    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(num_receivers,
                                                              config::local_exchange_buffer_mem_limit_per_driver);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    inherit_upstream_source_properties(local_exchange_source.get(), source_operator(pred_operators));
    local_exchange_source->set_could_local_shuffle(true);
    local_exchange_source->set_degree_of_parallelism(num_receivers);

    auto local_exchange = std::make_shared<BroadcastExchanger>(mem_mgr, local_exchange_source.get());
    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, local_exchange);
    pred_operators.emplace_back(std::move(local_exchange_sink));
    add_pipeline(pred_operators);

    return {std::move(local_exchange_source)};
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(RuntimeState* state,
                                                                                 int32_t plan_node_id,
                                                                                 OpFactories& pred_operators) {
    return maybe_interpolate_local_passthrough_exchange(state, plan_node_id, pred_operators, 1);
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(RuntimeState* state,
                                                                                 int32_t plan_node_id,
                                                                                 OpFactories& pred_operators,
                                                                                 int num_receivers, bool force) {
    return _maybe_interpolate_local_passthrough_exchange(state, plan_node_id, pred_operators, num_receivers, force,
                                                         LocalExchanger::PassThroughType::CHUNK);
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_random_passthrough_exchange(RuntimeState* state,
                                                                                        int32_t plan_node_id,
                                                                                        OpFactories& pred_operators,
                                                                                        int num_receivers, bool force) {
    return _maybe_interpolate_local_passthrough_exchange(state, plan_node_id, pred_operators, num_receivers, force,
                                                         LocalExchanger::PassThroughType::RANDOM);
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_adpative_passthrough_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators, int num_receivers, bool force) {
    return _maybe_interpolate_local_passthrough_exchange(state, plan_node_id, pred_operators, num_receivers, force,
                                                         LocalExchanger::PassThroughType::ADPATIVE);
}

OpFactories PipelineBuilderContext::_maybe_interpolate_local_passthrough_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators, int num_receivers, bool force,
        LocalExchanger::PassThroughType pass_through_type) {
    // predecessor pipeline has multiple drivers that will produce multiple output streams, but sort operator is
    // not parallelized now and can not accept multiple streams as input, so add a LocalExchange to gather multiple
    // streams and produce one output stream piping into the sort operator.
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());
    auto* source_op = source_operator(pred_operators);
    if (!force && source_op->degree_of_parallelism() == num_receivers && !source_op->is_skewed()) {
        return pred_operators;
    }

    int max_input_dop = std::max(num_receivers, static_cast<int>(source_op->degree_of_parallelism()));
    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(max_input_dop,
                                                              config::local_exchange_buffer_mem_limit_per_driver);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    inherit_upstream_source_properties(local_exchange_source.get(), source_op);
    local_exchange_source->set_could_local_shuffle(true);
    local_exchange_source->set_degree_of_parallelism(num_receivers);

    std::shared_ptr<LocalExchanger> local_exchange;
    if (pass_through_type == LocalExchanger::PassThroughType::ADPATIVE) {
        local_exchange = std::make_shared<AdaptivePassthroughExchanger>(mem_mgr, local_exchange_source.get());
    } else if (pass_through_type == LocalExchanger::PassThroughType::RANDOM) {
        local_exchange = std::make_shared<RandomPassthroughExchanger>(mem_mgr, local_exchange_source.get());
    } else {
        local_exchange = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());
    }
    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, local_exchange);
    pred_operators.emplace_back(std::move(local_exchange_sink));
    add_pipeline(pred_operators);

    return {std::move(local_exchange_source)};
}

void PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange_for_sink(RuntimeState* state,
                                                                                   int32_t plan_node_id,
                                                                                   OpFactoryPtr table_sink_operator,
                                                                                   int32_t source_operator_dop,
                                                                                   int32_t desired_sink_dop) {
    if (source_operator_dop == desired_sink_dop) {
        return;
    }

    auto* source_operator =
            down_cast<SourceOperatorFactory*>(_fragment_context->pipelines().back()->source_operator_factory());
    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(source_operator_dop,
                                                              config::local_exchange_buffer_mem_limit_per_driver);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    auto exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());

    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, exchanger);
    _fragment_context->pipelines().back()->add_op_factory(local_exchange_sink);

    local_exchange_source->set_degree_of_parallelism(desired_sink_dop);
    local_exchange_source->set_runtime_state(state);
    local_exchange_source->set_group_leader(source_operator);

    OpFactories operators_source_with_local_exchange{std::move(local_exchange_source), std::move(table_sink_operator)};

    auto pipeline_with_local_exchange_source =
            std::make_shared<Pipeline>(next_pipe_id(), operators_source_with_local_exchange);
    _fragment_context->pipelines().emplace_back(std::move(pipeline_with_local_exchange_source));
}

void PipelineBuilderContext::maybe_interpolate_local_key_partition_exchange_for_sink(
        RuntimeState* state, int32_t plan_node_id, OpFactoryPtr table_sink_operator,
        const std::vector<ExprContext*>& partition_expr_ctxs, int32_t source_operator_dop, int32_t desired_sink_dop) {
    auto* source_operator =
            down_cast<SourceOperatorFactory*>(_fragment_context->pipelines().back()->source_operator_factory());
    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(
            source_operator_dop * state->chunk_size() * localExchangeBufferChunks(),
            config::local_exchange_buffer_mem_limit_per_driver);
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    auto local_exchanger = std::make_shared<KeyPartitionExchanger>(mem_mgr, local_shuffle_source.get(),
                                                                   partition_expr_ctxs, source_operator_dop);

    auto local_shuffle_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, local_exchanger);

    _fragment_context->pipelines().back()->add_op_factory(local_shuffle_sink);

    local_shuffle_source->set_runtime_state(state);
    local_shuffle_source->set_group_leader(source_operator);
    local_shuffle_source->set_degree_of_parallelism(desired_sink_dop);
    OpFactories operators_source_with_local_shuffle{std::move(local_shuffle_source), std::move(table_sink_operator)};

    auto pipeline_with_local_exchange_source =
            std::make_shared<Pipeline>(next_pipe_id(), operators_source_with_local_shuffle);
    _fragment_context->pipelines().emplace_back(std::move(pipeline_with_local_exchange_source));
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_shuffle_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
        const std::vector<ExprContext*>& self_partition_exprs) {
    return maybe_interpolate_local_shuffle_exchange(state, plan_node_id, pred_operators,
                                                    [&self_partition_exprs]() { return self_partition_exprs; });
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_shuffle_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
        const PartitionExprsGenerator& self_partition_exprs_generator) {
    auto* source_op = source_operator(pred_operators);
    if (!source_op->could_local_shuffle()) {
        return pred_operators;
    }

    if (!source_op->partition_exprs().empty()) {
        return _do_maybe_interpolate_local_shuffle_exchange(state, plan_node_id, pred_operators,
                                                            source_op->partition_exprs(), source_op->partition_type());
    }

    return _do_maybe_interpolate_local_shuffle_exchange(state, plan_node_id, pred_operators,
                                                        self_partition_exprs_generator(), source_op->partition_type());
}

OpFactories PipelineBuilderContext::_do_maybe_interpolate_local_shuffle_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
        const std::vector<ExprContext*>& partition_expr_ctxs, const TPartitionType::type part_type) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());

    // If DOP is one, we needn't partition input chunks.
    size_t shuffle_partitions_num = degree_of_parallelism();
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    auto* pred_source_op = source_operator(pred_operators);

    // To make sure at least one partition source operator is ready to output chunk before sink operators are full.
    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(shuffle_partitions_num,
                                                              config::local_exchange_buffer_mem_limit_per_driver);
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    local_shuffle_source->set_runtime_state(state);
    inherit_upstream_source_properties(local_shuffle_source.get(), pred_source_op);
    local_shuffle_source->set_could_local_shuffle(pred_source_op->partition_exprs().empty());
    local_shuffle_source->set_degree_of_parallelism(shuffle_partitions_num);

    auto local_shuffle =
            std::make_shared<PartitionExchanger>(mem_mgr, local_shuffle_source.get(), part_type, partition_expr_ctxs);
    auto local_shuffle_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, local_shuffle);
    pred_operators.emplace_back(std::move(local_shuffle_sink));
    add_pipeline(pred_operators);

    return {std::move(local_shuffle_source)};
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_ordered_partition_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
        const std::vector<ExprContext*>& partition_expr_ctxs) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());

    // If DOP is one, we needn't partition input chunks.
    size_t shuffle_partitions_num = degree_of_parallelism();
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    auto* pred_source_op = source_operator(pred_operators);

    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(shuffle_partitions_num,
                                                              config::local_exchange_buffer_mem_limit_per_driver);
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    local_shuffle_source->set_runtime_state(state);
    inherit_upstream_source_properties(local_shuffle_source.get(), pred_source_op);
    local_shuffle_source->set_could_local_shuffle(pred_source_op->partition_exprs().empty());
    local_shuffle_source->set_degree_of_parallelism(shuffle_partitions_num);

    auto local_shuffle =
            std::make_shared<OrderedPartitionExchanger>(mem_mgr, local_shuffle_source.get(), partition_expr_ctxs);
    auto local_shuffle_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, local_shuffle);

    pred_operators.emplace_back(std::move(local_shuffle_sink));
    add_pipeline(pred_operators);

    return {std::move(local_shuffle_source)};
}

void PipelineBuilderContext::interpolate_spill_process(size_t plan_node_id,
                                                       const SpillProcessChannelFactoryPtr& spill_channel_factory,
                                                       size_t dop) {
    OpFactories spill_process_operators;
    auto spill_process_factory = std::make_shared<SpillProcessOperatorFactory>(next_operator_id(), "spill_process",
                                                                               plan_node_id, spill_channel_factory);
    spill_process_factory->set_degree_of_parallelism(dop);
    spill_process_operators.emplace_back(std::move(spill_process_factory));
    auto noop_sink_factory = std::make_shared<NoopSinkOperatorFactory>(next_operator_id(), plan_node_id);
    spill_process_operators.emplace_back(std::move(noop_sink_factory));
    add_pipeline(std::move(spill_process_operators));
}

OpFactories PipelineBuilderContext::maybe_gather_pipelines_to_one(RuntimeState* state, int32_t plan_node_id,
                                                                  std::vector<OpFactories>& pred_operators_list) {
    // If there is only one pred pipeline, we needn't local passthrough anymore.
    if (pred_operators_list.size() == 1) {
        return pred_operators_list[0];
    }

    // Approximately, each pred driver can output state->chunk_size() rows at the same time.
    size_t max_input_dop = 0;
    for (const auto& pred_ops : pred_operators_list) {
        auto* source_op = source_operator(pred_ops);
        max_input_dop += source_op->degree_of_parallelism();
    }

    auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(max_input_dop,
                                                              config::local_exchange_buffer_mem_limit_per_driver);
    auto local_exchange_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    local_exchange_source->set_runtime_state(state);
    inherit_upstream_source_properties(local_exchange_source.get(), source_operator(pred_operators_list[0]));
    local_exchange_source->set_could_local_shuffle(true);
    local_exchange_source->set_degree_of_parallelism(degree_of_parallelism());

    auto exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());
    for (auto& pred_operators : pred_operators_list) {
        auto local_exchange_sink =
                std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, exchanger);
        pred_operators.emplace_back(std::move(local_exchange_sink));
        add_pipeline(pred_operators);
    }

    return {std::move(local_exchange_source)};
}

OpFactories PipelineBuilderContext::maybe_interpolate_collect_stats(RuntimeState* state, int32_t plan_node_id,
                                                                    OpFactories& pred_operators) {
    if (_force_disable_adaptive_dop || !_fragment_context->enable_adaptive_dop()) {
        return pred_operators;
    }

    if (pred_operators.empty()) {
        return pred_operators;
    }

    auto* pred_source_op = source_operator(pred_operators);
    size_t dop = pred_source_op->degree_of_parallelism();
    CollectStatsContextPtr collect_stats_ctx =
            std::make_shared<CollectStatsContext>(state, dop, _fragment_context->adaptive_dop_param());

    auto last_plan_node_id = pred_operators[pred_operators.size() - 1]->plan_node_id();
    pred_operators.emplace_back(std::make_shared<CollectStatsSinkOperatorFactory>(next_operator_id(), last_plan_node_id,
                                                                                  collect_stats_ctx));
    add_pipeline(std::move(pred_operators));

    auto downstream_source_op = std::make_shared<CollectStatsSourceOperatorFactory>(
            next_operator_id(), last_plan_node_id, std::move(collect_stats_ctx));
    inherit_upstream_source_properties(downstream_source_op.get(), pred_source_op);
    downstream_source_op->set_partition_exprs(pred_source_op->partition_exprs());

    for (const auto& pipeline : _dependent_pipelines) {
        downstream_source_op->add_group_dependent_pipeline(pipeline);
    }

    return {std::move(downstream_source_op)};
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

SourceOperatorFactory* PipelineBuilderContext::source_operator(OpFactories ops) {
    return down_cast<SourceOperatorFactory*>(ops[0].get());
}

bool PipelineBuilderContext::could_local_shuffle(OpFactories ops) const {
    return down_cast<SourceOperatorFactory*>(ops[0].get())->could_local_shuffle();
}

bool PipelineBuilderContext::should_interpolate_cache_operator(int32_t plan_node_id, OpFactoryPtr& source_op) {
    if (!_fragment_context->enable_cache()) {
        return false;
    }
    const auto& cache_param = _fragment_context->cache_param();
    if (cache_param.plan_node_id != plan_node_id) {
        return false;
    }
    return dynamic_cast<pipeline::OperatorFactory*>(source_op.get()) != nullptr;
}

OpFactories PipelineBuilderContext::interpolate_cache_operator(
        int32_t plan_node_id, OpFactories& upstream_pipeline, OpFactories& downstream_pipeline,
        const std::function<std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>(bool)>& merge_operators_generator) {
    DCHECK(should_interpolate_cache_operator(downstream_pipeline[0]->plan_node_id(), upstream_pipeline[0]));

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
    auto cache_op = std::make_shared<query_cache::CacheOperatorFactory>(next_operator_id(), plan_node_id, cache_mgr,
                                                                        cache_param);
    upstream_pipeline.push_back(cache_op);

    auto merge_operators = merge_operators_generator(true);
    upstream_pipeline.push_back(std::move(std::get<0>(merge_operators)));
    downstream_pipeline.push_back(std::move(std::get<1>(merge_operators)));
    down_cast<SourceOperatorFactory*>(downstream_pipeline.front().get())->set_degree_of_parallelism(dop);
    return downstream_pipeline;
}

void PipelineBuilderContext::inherit_upstream_source_properties(SourceOperatorFactory* downstream_source,
                                                                SourceOperatorFactory* upstream_source) {
    downstream_source->set_degree_of_parallelism(upstream_source->degree_of_parallelism());
    downstream_source->set_could_local_shuffle(upstream_source->could_local_shuffle());
    downstream_source->set_partition_type(upstream_source->partition_type());
    if (!upstream_source->partition_exprs().empty() || !downstream_source->partition_exprs().empty()) {
        downstream_source->set_partition_exprs(upstream_source->partition_exprs());
    }

    if (downstream_source->adaptive_initial_state() != SourceOperatorFactory::AdaptiveState::NONE) {
        downstream_source->set_group_leader(downstream_source);
    } else {
        downstream_source->set_group_leader(upstream_source);
    }
}

void PipelineBuilderContext::push_dependent_pipeline(const Pipeline* pipeline) {
    _dependent_pipelines.emplace_back(pipeline);
}
void PipelineBuilderContext::pop_dependent_pipeline() {
    _dependent_pipelines.pop_back();
}

/// PipelineBuilder.
Pipelines PipelineBuilder::build(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    _context.add_pipeline(operators);
    return _context.get_pipelines();
}

} // namespace starrocks::pipeline
