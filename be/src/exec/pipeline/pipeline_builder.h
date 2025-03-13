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

#pragma once

#include <unordered_map>

#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/group_execution/execution_group_builder.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/spill_process_channel.h"

namespace starrocks {
class ExecNode;
namespace pipeline {

class PipelineBuilderContext {
public:
    PipelineBuilderContext(FragmentContext* fragment_context, size_t degree_of_parallelism, size_t sink_dop,
                           bool is_stream_pipeline)
            : _fragment_context(fragment_context),
              _degree_of_parallelism(degree_of_parallelism),
              _data_sink_dop(sink_dop),
              _is_stream_pipeline(is_stream_pipeline),
              _enable_group_execution(fragment_context->enable_group_execution()) {
        // init the default execution group
        _execution_groups.emplace_back(ExecutionGroupBuilder::create_normal_exec_group());
        _normal_exec_group = _execution_groups.back().get();
        _current_execution_group = _execution_groups.back().get();
    }

    void init_colocate_groups(std::unordered_map<int32_t, ExecutionGroupPtr>&& colocate_groups);
    ExecutionGroupRawPtr find_exec_group_by_plan_node_id(int32_t plan_node_id);
    void set_current_execution_group(ExecutionGroupRawPtr exec_group) { _current_execution_group = exec_group; }
    ExecutionGroupRawPtr current_execution_group() { return _current_execution_group; }

    void add_pipeline(const OpFactories& operators, ExecutionGroupRawPtr execution_group) {
        // TODO: refactor Pipelines to PipelineRawPtrs
        _pipelines.emplace_back(std::make_shared<Pipeline>(next_pipe_id(), operators, execution_group));
        execution_group->add_pipeline(_pipelines.back().get());
        _subscribe_pipeline_event(_pipelines.back().get());
    }

    void add_pipeline(const OpFactories& operators) { add_pipeline(operators, _current_execution_group); }

    void add_independent_pipeline(const OpFactories& operators) { add_pipeline(operators, _normal_exec_group); }

    bool is_colocate_group() const { return _current_execution_group->type() == ExecutionGroupType::COLOCATE; }

    OpFactories maybe_interpolate_local_broadcast_exchange(RuntimeState* state, int32_t plan_node_id,
                                                           OpFactories& pred_operators, int num_receivers);

    // Input the output chunks from the drivers of pred operators into ONE driver of the post operators.
    OpFactories maybe_interpolate_local_passthrough_exchange(RuntimeState* state, int32_t plan_node_id,
                                                             OpFactories& pred_operators);
    OpFactories maybe_interpolate_local_passthrough_exchange(RuntimeState* state, int32_t plan_node_id,
                                                             OpFactories& pred_operators, int num_receivers,
                                                             bool force = false);
    OpFactories maybe_interpolate_local_passthrough_exchange(RuntimeState* state, int32_t plan_node_id,
                                                             OpFactories& pred_operators, int num_receivers,
                                                             LocalExchanger::PassThroughType pass_through_type);
    OpFactories maybe_interpolate_local_random_passthrough_exchange(RuntimeState* state, int32_t plan_node_id,
                                                                    OpFactories& pred_operators, int num_receivers,
                                                                    bool force = false);
    OpFactories maybe_interpolate_local_adpative_passthrough_exchange(RuntimeState* state, int32_t plan_node_id,
                                                                      OpFactories& pred_operators, int num_receivers,
                                                                      bool force = false);
    // using KeyPartitionExchanger
    // interpolate local shuffle exchange with partition_exprs
    OpFactories interpolate_local_key_partition_exchange(RuntimeState* state, int32_t plan_node_id,
                                                         OpFactories& pred_operators,
                                                         const std::vector<ExprContext*>& partition_expr_ctxs,
                                                         int num_receivers);
    /// Local shuffle the output chunks from multiple drivers of pred operators into DOP partitions of the post operators.
    /// The partition is generated by evaluated each row via partition_expr_ctxs.
    /// When interpolating a local shuffle?
    /// - Local shuffle is interpolated only when DOP > 1 and the source operator of pred_operators could local shuffle.
    /// partition_exprs
    /// - If the source operator has a partition exprs, use it as partition_exprs.
    /// - Otherwise, use self_partition_exprs or self_partition_exprs_generator().
    OpFactories maybe_interpolate_local_shuffle_exchange(RuntimeState* state, int32_t plan_node_id,
                                                         OpFactories& pred_operators,
                                                         const std::vector<ExprContext*>& self_partition_exprs);
    using PartitionExprsGenerator = std::function<std::vector<ExprContext*>()>;
    OpFactories maybe_interpolate_local_shuffle_exchange(RuntimeState* state, int32_t plan_node_id,
                                                         OpFactories& pred_operators,
                                                         const PartitionExprsGenerator& self_partition_exprs_generator);

    // The intput data is already ordered by partition_exprs. Then we can use a simply approach to split them into different channels
    // as long as the data of the same partition_exprs are in the same channel.
    OpFactories maybe_interpolate_local_ordered_partition_exchange(
            RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
            const std::vector<ExprContext*>& partition_expr_ctxs);

    void interpolate_spill_process(size_t plan_node_id, const SpillProcessChannelFactoryPtr& channel_factory,
                                   size_t dop);

    OpFactories interpolate_grouped_exchange(int32_t plan_node_id, OpFactories& pred_operators);
    OpFactories maybe_interpolate_grouped_exchange(int32_t plan_node_id, OpFactories& pred_operators);

    // Uses local exchange to gather the output chunks of multiple predecessor pipelines
    // into a new pipeline, which the successor operator belongs to.
    // Append a LocalExchangeSinkOperator to the tail of each pipeline.
    // Create a new pipeline with a LocalExchangeSourceOperator.
    // These local exchange sink operators and the source operator share a passthrough exchanger.
    // pass_through_type specify how to move chunk from prev pipeline to new pipeline.
    OpFactories maybe_gather_pipelines_to_one(RuntimeState* state, int32_t plan_node_id,
                                              std::vector<OpFactories>& pred_operators_list,
                                              LocalExchanger::PassThroughType pass_through_type);

    OpFactories maybe_interpolate_collect_stats(RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators);

    OpFactories maybe_interpolate_debug_ops(RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators);

    uint32_t next_pipe_id() { return _next_pipeline_id++; }

    uint32_t next_operator_id() { return _next_operator_id++; }

    size_t degree_of_parallelism() const { return _degree_of_parallelism; }
    size_t data_sink_dop() const { return _data_sink_dop; }

    bool is_stream_pipeline() const { return _is_stream_pipeline; }

    const Pipeline* last_pipeline() const {
        DCHECK(!_pipelines.empty());
        return _pipelines[_pipelines.size() - 1].get();
    }

    RuntimeState* runtime_state() { return _fragment_context->runtime_state(); }
    FragmentContext* fragment_context() { return _fragment_context; }
    bool enable_group_execution() const { return _enable_group_execution; }

    size_t dop_of_source_operator(int source_node_id);
    MorselQueueFactory* morsel_queue_factory_of_source_operator(int source_node_id);
    MorselQueueFactory* morsel_queue_factory_of_source_operator(const SourceOperatorFactory* source_op);
    SourceOperatorFactory* source_operator(const OpFactories& ops);
    // Whether the building pipeline `ops` need local shuffle for the next operator.
    bool could_local_shuffle(const OpFactories& ops) const;

    bool should_interpolate_cache_operator(int32_t plan_node_id, OpFactoryPtr& source_op);
    OpFactories interpolate_cache_operator(
            int32_t plan_node_id, OpFactories& upstream_pipeline, OpFactories& downstream_pipeline,
            const std::function<std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>(bool)>& merge_operators_generator);

    // help to change some actions after aggregations, for example,
    // disable to ignore local data after aggregations with profile exchange speed.
    bool has_aggregation = false;

    static int localExchangeBufferChunks() { return kLocalExchangeBufferChunks; }

    void inherit_upstream_source_properties(SourceOperatorFactory* downstream_source,
                                            SourceOperatorFactory* upstream_source);

    void push_dependent_pipeline(const Pipeline* pipeline);
    void pop_dependent_pipeline();

    ExecutionGroups execution_groups() { return std::move(_execution_groups); }
    Pipelines pipelines() { return std::move(_pipelines); }

private:
    // op1->limit->op2 (except accumulate) => return -1
    // op1->limit->op2 (accumulate) => return limit
    int64_t _prev_limit_size(const OpFactories& pred_operators);
    void _try_interpolate_limit_operator(int32_t plan_node_id, OpFactories& pred_operators, int64_t limit_size);

    void _subscribe_pipeline_event(Pipeline* pipeline);

    OpFactories _maybe_interpolate_local_passthrough_exchange(RuntimeState* state, int32_t plan_node_id,
                                                              OpFactories& pred_operators, int num_receivers,
                                                              bool force,
                                                              LocalExchanger::PassThroughType pass_through_type);

    OpFactories _do_maybe_interpolate_local_shuffle_exchange(
            RuntimeState* state, int32_t plan_node_id, OpFactories& pred_operators,
            const std::vector<ExprContext*>& partition_expr_ctxs,
            const TPartitionType::type part_type = TPartitionType::type::HASH_PARTITIONED);

    static constexpr int kLocalExchangeBufferChunks = 8;

    FragmentContext* _fragment_context;
    Pipelines _pipelines;
    ExecutionGroups _execution_groups;
    std::unordered_map<int32_t, ExecutionGroupPtr> _group_id_to_colocate_groups;
    ExecutionGroupRawPtr _normal_exec_group = nullptr;
    ExecutionGroupRawPtr _current_execution_group = nullptr;

    std::list<const Pipeline*> _dependent_pipelines;

    uint32_t _next_pipeline_id = 0;
    uint32_t _next_operator_id = 0;

    const size_t _degree_of_parallelism;
    const size_t _data_sink_dop;

    const bool _is_stream_pipeline;
    const bool _enable_group_execution;
};

class PipelineBuilder {
public:
    explicit PipelineBuilder(PipelineBuilderContext& context) : _context(context) {}

    // Build pipeline from exec node tree
    OpFactories decompose_exec_node_to_pipeline(const FragmentContext& fragment, ExecNode* exec_node);

    std::pair<ExecutionGroups, Pipelines> build();

private:
    PipelineBuilderContext& _context;
};
} // namespace pipeline
} // namespace starrocks
