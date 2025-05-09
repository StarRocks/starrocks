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

#include <memory>

#include "adaptive/event.h"
#include "common/config.h"
#include "exec/exec_node.h"
#include "exec/pipeline/adaptive/collect_stats_context.h"
#include "exec/pipeline/adaptive/collect_stats_sink_operator.h"
#include "exec/pipeline/adaptive/collect_stats_source_operator.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/group_execution/group_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/spill_process_operator.h"
#include "exec/pipeline/wait_operator.h"
#include "exec/query_cache/cache_manager.h"
#include "exec/query_cache/cache_operator.h"
#include "exec/query_cache/conjugate_operator.h"
#include "exec/query_cache/lane_arbiter.h"
#include "exec/query_cache/multilane_operator.h"

namespace starrocks::pipeline {

void PipelineBuilderContext::init_colocate_groups(std::unordered_map<int32_t, ExecutionGroupPtr>&& colocate_groups) {
    _group_id_to_colocate_groups = std::move(colocate_groups);
    for (auto& [group_id, group] : _group_id_to_colocate_groups) {
        _execution_groups.emplace_back(group);
    }
}

ExecutionGroupRawPtr PipelineBuilderContext::find_exec_group_by_plan_node_id(int32_t plan_node_id) {
    for (auto& [group_id, group] : _group_id_to_colocate_groups) {
        if (group->contains(plan_node_id)) {
            return group.get();
        }
    }
    return _normal_exec_group;
}

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

OpFactories PipelineBuilderContext::maybe_interpolate_local_passthrough_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators, int num_receivers,
        LocalExchanger::PassThroughType pass_through_type) {
    return _maybe_interpolate_local_passthrough_exchange(state, plan_node_id, pred_operators, num_receivers, false,
                                                         pass_through_type);
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

    pred_operators = maybe_interpolate_grouped_exchange(plan_node_id, pred_operators);

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
    } else if (state->query_options().__isset.enable_connector_sink_writer_scaling &&
               state->query_options().enable_connector_sink_writer_scaling &&
               pass_through_type == LocalExchanger::PassThroughType::SCALE) {
        local_exchange = std::make_shared<ConnectorSinkPassthroughExchanger>(mem_mgr, local_exchange_source.get());
    } else {
        local_exchange = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());
    }
    auto local_exchange_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, local_exchange);
    pred_operators.emplace_back(std::move(local_exchange_sink));
    add_pipeline(pred_operators);

    return {std::move(local_exchange_source)};
}

OpFactories PipelineBuilderContext::interpolate_local_key_partition_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
        const std::vector<ExprContext*>& partition_expr_ctxs, int num_receivers) {
    auto* pred_source_op = source_operator(pred_operators);
    size_t source_dop = pred_source_op->degree_of_parallelism();
    auto mem_mgr =
            std::make_shared<ChunkBufferMemoryManager>(source_dop, config::local_exchange_buffer_mem_limit_per_driver);
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    auto local_exchanger = std::make_shared<KeyPartitionExchanger>(mem_mgr, local_shuffle_source.get(),
                                                                   partition_expr_ctxs, source_dop);
    auto local_shuffle_sink =
            std::make_shared<LocalExchangeSinkOperatorFactory>(next_operator_id(), plan_node_id, local_exchanger);
    pred_operators.emplace_back(std::move(local_shuffle_sink));
    add_pipeline(pred_operators);

    local_shuffle_source->set_runtime_state(state);
    inherit_upstream_source_properties(local_shuffle_source.get(), pred_source_op);
    local_shuffle_source->set_degree_of_parallelism(num_receivers);

    return {std::move(local_shuffle_source)};
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

    // interpolate grouped exchange if needed
    // TODO: If the local exchange supports spills, we don't need to prevent group execution
    pred_operators = maybe_interpolate_grouped_exchange(plan_node_id, pred_operators);

    // If DOP is one, we needn't partition input chunks.
    size_t shuffle_partitions_num = degree_of_parallelism();
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    auto* pred_source_op = source_operator(pred_operators);
    int64_t limit_size = _prev_limit_size(pred_operators);

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

    OpFactories source_operators = {std::move(local_shuffle_source)};
    _try_interpolate_limit_operator(plan_node_id, source_operators, limit_size);
    return source_operators;
}

OpFactories PipelineBuilderContext::maybe_interpolate_local_ordered_partition_exchange(
        RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
        const std::vector<ExprContext*>& partition_expr_ctxs) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());

    // interpolate grouped exchange if needed
    // TODO: If the local exchange supports spills, we don't need to prevent group execution
    pred_operators = maybe_interpolate_grouped_exchange(plan_node_id, pred_operators);

    // If DOP is one, we needn't partition input chunks.
    size_t shuffle_partitions_num = degree_of_parallelism();
    if (shuffle_partitions_num <= 1) {
        return pred_operators;
    }

    auto* pred_source_op = source_operator(pred_operators);
    int64_t limit_size = _prev_limit_size(pred_operators);

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

    OpFactories source_operators = {std::move(local_shuffle_source)};
    _try_interpolate_limit_operator(plan_node_id, source_operators, limit_size);
    return source_operators;
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

OpFactories PipelineBuilderContext::interpolate_grouped_exchange(int32_t plan_node_id, OpFactories& pred_operators) {
    size_t physical_dop = degree_of_parallelism();
    auto* source_op = source_operator(pred_operators);
    int logical_dop = source_op->degree_of_parallelism();

    // check should interpolate limit operator
    int64_t limit_size = _prev_limit_size(pred_operators);

    auto mem_mgr =
            std::make_shared<ChunkBufferMemoryManager>(logical_dop, config::local_exchange_buffer_mem_limit_per_driver);
    auto local_shuffle_source =
            std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), plan_node_id, mem_mgr);
    auto local_exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_shuffle_source.get());
    auto group_exchange_sink = std::make_shared<GroupedExecutionSinkFactory>(next_operator_id(), plan_node_id,
                                                                             local_exchanger, _current_execution_group);
    pred_operators.emplace_back(std::move(group_exchange_sink));

    auto prev_source_operator = source_operator(pred_operators);
    inherit_upstream_source_properties(local_shuffle_source.get(), prev_source_operator);
    local_shuffle_source->set_could_local_shuffle(true);
    local_shuffle_source->set_degree_of_parallelism(physical_dop);
    add_pipeline(pred_operators);
    // switch to new normal group
    _current_execution_group = _normal_exec_group;

    OpFactories source_operators = {std::move(local_shuffle_source)};
    _try_interpolate_limit_operator(plan_node_id, source_operators, limit_size);
    return source_operators;
}

OpFactories PipelineBuilderContext::maybe_interpolate_grouped_exchange(int32_t plan_node_id,
                                                                       OpFactories& pred_operators) {
    if (dynamic_cast<ColocateExecutionGroup*>(_current_execution_group) != nullptr) {
        return interpolate_grouped_exchange(plan_node_id, pred_operators);
    }
    return pred_operators;
}

OpFactories PipelineBuilderContext::maybe_gather_pipelines_to_one(RuntimeState* state, int32_t plan_node_id,
                                                                  std::vector<OpFactories>& pred_operators_list,
                                                                  LocalExchanger::PassThroughType pass_through_type) {
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
    auto* first_upstream_source = source_operator(pred_operators_list[0]);
    inherit_upstream_source_properties(local_exchange_source.get(), first_upstream_source);
    local_exchange_source->set_could_local_shuffle(true);
    local_exchange_source->set_degree_of_parallelism(degree_of_parallelism());

    std::vector<EventPtr> group_blocking_events;
    for (const auto& pred_ops : pred_operators_list) {
        auto* source = source_operator(pred_ops);
        if (auto event = source->group_leader()->adaptive_blocking_event(); event != nullptr) {
            group_blocking_events.emplace_back(std::move(event));
        }
    }

    for (int i = 1; i < pred_operators_list.size(); i++) {
        auto* upstream_source = source_operator(pred_operators_list[i]);
        local_exchange_source->add_upstream_source(upstream_source);
        first_upstream_source->union_group(upstream_source);
    }

    if (!group_blocking_events.empty()) {
        EventPtr merged_blocking_events = Event::depends_all(group_blocking_events);
        local_exchange_source->group_leader()->set_adaptive_blocking_event(std::move(merged_blocking_events));
    }

    std::shared_ptr<LocalExchanger> exchanger;
    if (pass_through_type == LocalExchanger::PassThroughType::RANDOM) {
        exchanger = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());
    } else {
        exchanger = std::make_shared<DirectThroughExchanger>(mem_mgr, local_exchange_source.get());
    }

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
    if (!_fragment_context->enable_adaptive_dop()) {
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

OpFactories PipelineBuilderContext::maybe_interpolate_debug_ops(RuntimeState* state, int32_t plan_node_id,
                                                                OpFactories& pred_operators) {
    auto action_opt = runtime_state()->debug_action_mgr().get_debug_action(plan_node_id);
    if (action_opt.has_value() && action_opt.value().is_wait_action()) {
        auto* pred_source_op = source_operator(pred_operators);
        auto wait_context_factory = std::make_shared<WaitContextFactory>(action_opt->value);
        auto wait_sink =
                std::make_shared<WaitOperatorSinkFactory>(next_operator_id(), plan_node_id, wait_context_factory);

        pred_operators.push_back(std::move(wait_sink));
        add_pipeline(pred_operators);

        auto wait_src =
                std::make_shared<WaitOperatorSourceFactory>(next_operator_id(), plan_node_id, wait_context_factory);
        this->inherit_upstream_source_properties(wait_src.get(), pred_source_op);
        return {std::move(wait_src)};
    }
    return pred_operators;
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

SourceOperatorFactory* PipelineBuilderContext::source_operator(const OpFactories& ops) {
    return down_cast<SourceOperatorFactory*>(ops[0].get());
}

bool PipelineBuilderContext::could_local_shuffle(const OpFactories& ops) const {
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

    if (downstream_source->adaptive_initial_state() == SourceOperatorFactory::AdaptiveState::NONE) {
        downstream_source->add_upstream_source(upstream_source);
    }
}

void PipelineBuilderContext::push_dependent_pipeline(const Pipeline* pipeline) {
    _dependent_pipelines.emplace_back(pipeline);
}
void PipelineBuilderContext::pop_dependent_pipeline() {
    _dependent_pipelines.pop_back();
}

int64_t PipelineBuilderContext::_prev_limit_size(const OpFactories& pred_operators) {
    for (auto it = pred_operators.rbegin(); it != pred_operators.rend(); ++it) {
        if (auto limit = dynamic_cast<LimitOperatorFactory*>(it->get())) {
            return limit->limit();
        } else if (dynamic_cast<ChunkAccumulateOperatorFactory*>(it->get()) == nullptr) {
            return -1;
        }
    }
    return -1;
}

void PipelineBuilderContext::_try_interpolate_limit_operator(int32_t plan_node_id, OpFactories& pred_operators,
                                                             int64_t limit_size) {
    if (limit_size >= 0 && limit_size < config::pipline_limit_max_delivery) {
        pred_operators.emplace_back(
                std::make_shared<LimitOperatorFactory>(next_operator_id(), plan_node_id, limit_size));
    }
}

void PipelineBuilderContext::_subscribe_pipeline_event(Pipeline* pipeline) {
    bool enable_wait_event = _fragment_context->runtime_state()->enable_wait_dependent_event();
    enable_wait_event &= !_current_execution_group->is_colocate_exec_group();
    if (enable_wait_event && !_dependent_pipelines.empty()) {
        pipeline->pipeline_event()->set_need_wait_dependencies_finished(true);
        pipeline->pipeline_event()->add_dependency(_dependent_pipelines.back()->pipeline_event());
    }
}

OpFactories PipelineBuilder::decompose_exec_node_to_pipeline(const FragmentContext& fragment, ExecNode* exec_node) {
    pipeline::OpFactories operators = exec_node->decompose_to_pipeline(&_context);
    operators = _context.maybe_interpolate_grouped_exchange(exec_node->id(), operators);
    return operators;
}

std::pair<ExecutionGroups, Pipelines> PipelineBuilder::build() {
    return {_context.execution_groups(), _context.pipelines()};
}

} // namespace starrocks::pipeline
