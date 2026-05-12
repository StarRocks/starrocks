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
#include "exec/pipeline/set/except_build_sink_operator.h"
#include "exec/pipeline/set/except_context.h"
#include "exec/pipeline/set/except_output_source_operator.h"
#include "exec/pipeline/set/except_probe_sink_operator.h"
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

    if (_local_partition_by_exprs.empty()) {
        ops_with_except_build_sink = context->maybe_interpolate_local_shuffle_exchange(
                runtime_state(), id(), ops_with_except_build_sink, _child_expr_lists[0]);
    } else {
        ops_with_except_build_sink = context->maybe_interpolate_local_bucket_shuffle_exchange(
                runtime_state(), id(), ops_with_except_build_sink, _local_partition_by_exprs[0]);
    }

    ops_with_except_build_sink.emplace_back(std::make_shared<ExceptBuildSinkOperatorFactory>(
            context->next_operator_id(), id(), except_partition_ctx_factory, _child_expr_lists[0]));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, ops_with_except_build_sink.back().get(), context,
                                               rc_rf_probe_collector);
    context->add_pipeline(ops_with_except_build_sink);
    context->push_dependent_pipeline(context->last_pipeline());
    DeferOp pop_dependent_pipeline([context]() { context->pop_dependent_pipeline(); });

    // Use the rest children to erase keys from the hash table by ExceptProbeSinkOperator.
    for (size_t i = 1; i < _children.size(); i++) {
        ASSIGN_OR_RETURN(auto ops_with_except_probe_sink, child(i)->decompose_to_pipeline(context));
        if (_local_partition_by_exprs.empty()) {
            ops_with_except_probe_sink = context->maybe_interpolate_local_shuffle_exchange(
                    runtime_state(), id(), ops_with_except_probe_sink, _child_expr_lists[i]);
        } else {
            ops_with_except_probe_sink = context->maybe_interpolate_local_bucket_shuffle_exchange(
                    runtime_state(), id(), ops_with_except_probe_sink, _local_partition_by_exprs[i]);
        }
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
