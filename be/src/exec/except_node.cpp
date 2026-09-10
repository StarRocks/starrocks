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

#include "exec/except_node.h"

#include "column/column_helper.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_builder_operators.h"
#include "exec/pipeline/set/except_build_sink_operator.h"
#include "exec/pipeline/set/except_context.h"
#include "exec/pipeline/set/except_output_source_operator.h"
#include "exec/pipeline/set/except_probe_sink_operator.h"
#include "exec/runtime/group_execution/execution_group.h"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks {

ExceptNode::ExceptNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs), _tuple_id(tnode.except_node.tuple_id) {}

Status ExceptNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    DCHECK_GE(_children.size(), 2);

    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& result_texpr_lists = tnode.except_node.result_expr_lists;
    for (auto& texprs : result_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, texprs, &ctxs, state));
        _child_expr_lists.push_back(ctxs);
    }

    if (tnode.except_node.__isset.local_partition_by_exprs) {
        auto& local_partition_by_exprs = tnode.except_node.local_partition_by_exprs;
        for (auto& texprs : local_partition_by_exprs) {
            std::vector<ExprContext*> ctxs;
            RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, texprs, &ctxs, state));
            _local_partition_by_exprs.push_back(ctxs);
        }
    }
    return Status::OK();
}

void ExceptNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    for (auto& exprs : _child_expr_lists) {
        ExprExecutor::close(exprs, state);
    }

    if (_build_pool != nullptr) {
        _build_pool->free_all();
    }

    if (_buffer_state != nullptr) {
        _buffer_state.reset();
    }

    if (_hash_set != nullptr) {
        _hash_set.reset();
    }

    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> ExceptNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    const auto num_operators_generated = _children.size() + 1;
    auto&& rc_rf_probe_collector =
            std::make_shared<RcRfProbeCollector>(num_operators_generated, std::move(this->runtime_filter_collector()));
    ExceptPartitionContextFactoryPtr except_partition_ctx_factory =
            std::make_shared<ExceptPartitionContextFactory>(_tuple_id, _children.size() - 1);

    // Use the first child to build the hast table by ExceptBuildSinkOperator.
    ASSIGN_OR_RETURN(auto ops_with_except_build_sink, child(0)->decompose_to_pipeline(context));

    // Every child of a set operation must be partitioned by the SAME scheme, or a key reaches a
    // different driver from each child and the two never meet in the partitioned hash set. Settle the
    // scheme once, from the build child, and hand it to all of them; each child still supplies its own
    // corresponding key exprs. Reading it per child is what broke: a scan whose bucket key is the set
    // key kept its bucket transform while a child behind a UNION had no bucket properties at all.
    //
    // Whether the children can be left alone is a property of all of them together, so decompose the
    // probe children before deciding anything. They used to be decomposed inside the dependent-pipeline
    // scope opened below, which is what made every pipeline built underneath them wait for the build
    // pipeline; subscribe_pipelines_since() puts that back for the ones built here.
    // Decomposing a child can leave a different execution group current, and each pipeline is
    // registered into whichever group is current at the time. Remember the group every child ends in
    // so each pipeline still lands where it did before the children were hoisted up here.
    auto* group_after_build_child = context->current_execution_group();
    const auto probe_children_begin = context->mark();
    std::vector<OpFactories> probe_child_ops(_children.size());
    std::vector<ExecutionGroupRawPtr> probe_child_groups(_children.size(), nullptr);
    for (size_t i = 1; i < _children.size(); i++) {
        ASSIGN_OR_RETURN(probe_child_ops[i], child(i)->decompose_to_pipeline(context));
        probe_child_groups[i] = context->current_execution_group();
    }
    const auto probe_children_end = context->mark();
    context->set_current_execution_group(group_after_build_child);

    const bool set_op_is_colocate = !_local_partition_by_exprs.empty();
    auto set_op_part_type =
            set_op_is_colocate ? TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED : TPartitionType::HASH_PARTITIONED;
    std::vector<TBucketProperty> set_op_bucket_properties;
    if (set_op_is_colocate) {
        set_op_bucket_properties = context->source_operator(ops_with_except_build_sink)->get_bucket_properties();
    }

    // A child that reports could_local_shuffle() == false has already had its rows assigned to
    // drivers upstream -- for a colocate set operation the FE hands each driver its own bucket
    // morsels -- and when that holds for EVERY child they are aligned on that assignment for free.
    // Both maybe_interpolate_local_shuffle_exchange and maybe_interpolate_local_bucket_shuffle_exchange
    // return early in that case, so the original code interpolated nothing at all, and forcing an
    // exchange on all of them would only add cost. Force the shared scheme only once some child would
    // be shuffled, which is where the children could disagree and be silently wrong.
    bool force_shuffle = context->source_operator(ops_with_except_build_sink)->could_local_shuffle();
    for (size_t i = 1; i < _children.size() && !force_shuffle; i++) {
        force_shuffle = context->source_operator(probe_child_ops[i])->could_local_shuffle();
    }

    auto partition_child = [&](OpFactories& ops, size_t i) {
        const auto& keys = set_op_is_colocate ? _local_partition_by_exprs[i] : _child_expr_lists[i];
        if (force_shuffle) {
            return ::starrocks::pipeline::builder::interpolate_local_forced_shuffle_exchange(
                    context, runtime_state(), id(), ops, keys, set_op_part_type, set_op_bucket_properties);
        }
        // No child can be locally shuffled: the original per-child call, which skips every one of them.
        return set_op_is_colocate ? ::starrocks::pipeline::builder::maybe_interpolate_local_bucket_shuffle_exchange(
                                            context, runtime_state(), id(), ops, keys)
                                  : ::starrocks::pipeline::builder::maybe_interpolate_local_shuffle_exchange(
                                            context, runtime_state(), id(), ops, keys);
    };

    ops_with_except_build_sink = partition_child(ops_with_except_build_sink, 0);

    ops_with_except_build_sink.emplace_back(std::make_shared<ExceptBuildSinkOperatorFactory>(
            context->next_operator_id(), id(), except_partition_ctx_factory, _child_expr_lists[0]));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, ops_with_except_build_sink.back().get(), context,
                                               rc_rf_probe_collector);
    context->add_pipeline(ops_with_except_build_sink);
    context->push_dependent_pipeline(context->last_pipeline());
    DeferOp pop_dependent_pipeline([context]() { context->pop_dependent_pipeline(); });
    context->bind_dependent_pipeline_between(probe_children_begin, probe_children_end, context->last_pipeline(),
                                             !context->current_execution_group()->is_colocate_exec_group());

    // Use the rest children to erase keys from the hash table by ExceptProbeSinkOperator.
    for (size_t i = 1; i < _children.size(); i++) {
        context->set_current_execution_group(probe_child_groups[i]);
        auto ops_with_except_probe_sink = partition_child(probe_child_ops[i], i);
        ops_with_except_probe_sink.emplace_back(std::make_shared<ExceptProbeSinkOperatorFactory>(
                context->next_operator_id(), id(), except_partition_ctx_factory, _child_expr_lists[i], i - 1));
        // Initialize OperatorFactory's fields involving runtime filters.
        pipeline::init_runtime_filter_for_operator(*this, ops_with_except_probe_sink.back().get(), context,
                                                   rc_rf_probe_collector);
        context->add_pipeline(ops_with_except_probe_sink);
    }

    // ExceptOutputSourceOperator is used to assemble the undeleted keys to output chunks.
    OpFactories ops_with_except_output_source;
    auto except_output_source = std::make_shared<ExceptOutputSourceOperatorFactory>(
            context->next_operator_id(), id(), except_partition_ctx_factory, _children.size() - 1);
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, except_output_source.get(), context, rc_rf_probe_collector);
    context->inherit_upstream_source_properties(except_output_source.get(),
                                                context->source_operator(ops_with_except_build_sink));
    ops_with_except_output_source.emplace_back(std::move(except_output_source));
    if (limit() != -1) {
        ops_with_except_output_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    return ops_with_except_output_source;
}

} // namespace starrocks
