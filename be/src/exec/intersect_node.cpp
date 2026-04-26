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

#include "exec/intersect_node.h"

#include <memory>

#include "column/column_helper.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/set/intersect_build_sink_operator.h"
#include "exec/pipeline/set/intersect_context.h"
#include "exec/pipeline/set/intersect_output_source_operator.h"
#include "exec/pipeline/set/intersect_probe_sink_operator.h"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks {

IntersectNode::IntersectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs), _tuple_id(tnode.intersect_node.tuple_id) {}

Status IntersectNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    DCHECK_GE(_children.size(), 2);
    _intersect_times = _children.size() - 1;
    _has_outer_join_child =
            tnode.intersect_node.__isset.has_outer_join_child && tnode.intersect_node.has_outer_join_child;

    // Create result_expr_ctx_lists_ from thrift exprs.
    const auto& result_texpr_lists = tnode.intersect_node.result_expr_lists;
    for (const auto& texprs : result_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, texprs, &ctxs, state));
        _child_expr_lists.push_back(ctxs);
    }

    if (tnode.intersect_node.__isset.local_partition_by_exprs) {
        auto& local_partition_by_exprs = tnode.intersect_node.local_partition_by_exprs;
        for (auto& texprs : local_partition_by_exprs) {
            std::vector<ExprContext*> ctxs;
            RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, texprs, &ctxs, state));
            _local_partition_by_exprs.push_back(ctxs);
        }
    }
    return Status::OK();
}

void IntersectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    for (auto& exprs : _child_expr_lists) {
        ExprExecutor::close(exprs, state);
    }

    if (_build_pool != nullptr) {
        _build_pool->free_all();
    }

    if (_hash_set != nullptr) {
        _hash_set.reset();
    }

    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> IntersectNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    IntersectPartitionContextFactoryPtr intersect_partition_ctx_factory =
            std::make_shared<IntersectPartitionContextFactory>(_tuple_id, _children.size() - 1);

    const auto num_operators_generated = _children.size() + 1;
    auto&& rc_rf_probe_collector =
            std::make_shared<RcRfProbeCollector>(num_operators_generated, std::move(this->runtime_filter_collector()));

    // Use the first child to build the hast table by IntersectBuildSinkOperator.
    ASSIGN_OR_RETURN(auto ops_with_intersect_build_sink, child(0)->decompose_to_pipeline(context));
    if (_local_partition_by_exprs.empty()) {
        ops_with_intersect_build_sink = context->maybe_interpolate_local_shuffle_exchange(
                runtime_state(), id(), ops_with_intersect_build_sink, _child_expr_lists[0]);
    } else {
        ops_with_intersect_build_sink = context->maybe_interpolate_local_bucket_shuffle_exchange(
                runtime_state(), id(), ops_with_intersect_build_sink, _local_partition_by_exprs[0]);
    }
    ops_with_intersect_build_sink.emplace_back(std::make_shared<IntersectBuildSinkOperatorFactory>(
            context->next_operator_id(), id(), intersect_partition_ctx_factory, _child_expr_lists[0],
            _has_outer_join_child));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, ops_with_intersect_build_sink.back().get(), context,
                                               rc_rf_probe_collector);
    context->add_pipeline(ops_with_intersect_build_sink);

    // Use the rest children to erase keys from the hast table by IntersectProbeSinkOperator.
    for (size_t i = 1; i < _children.size(); i++) {
        ASSIGN_OR_RETURN(auto ops_with_intersect_probe_sink, child(i)->decompose_to_pipeline(context));
        if (_local_partition_by_exprs.empty()) {
            ops_with_intersect_probe_sink = context->maybe_interpolate_local_shuffle_exchange(
                    runtime_state(), id(), ops_with_intersect_probe_sink, _child_expr_lists[i]);
        } else {
            ops_with_intersect_probe_sink = context->maybe_interpolate_local_bucket_shuffle_exchange(
                    runtime_state(), id(), ops_with_intersect_probe_sink, _local_partition_by_exprs[i]);
        }
        ops_with_intersect_probe_sink.emplace_back(std::make_shared<IntersectProbeSinkOperatorFactory>(
                context->next_operator_id(), id(), intersect_partition_ctx_factory, _child_expr_lists[i], i - 1));
        // Initialize OperatorFactory's fields involving runtime filters.
        pipeline::init_runtime_filter_for_operator(*this, ops_with_intersect_probe_sink.back().get(), context,
                                                   rc_rf_probe_collector);
        context->add_pipeline(ops_with_intersect_probe_sink);
    }

    // IntersectOutputSourceOperator is used to assemble the undeleted keys to output chunks.
    OpFactories operators_with_intersect_output_source;
    auto intersect_output_source = std::make_shared<IntersectOutputSourceOperatorFactory>(
            context->next_operator_id(), id(), intersect_partition_ctx_factory, _children.size() - 1);
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, intersect_output_source.get(), context, rc_rf_probe_collector);
    context->inherit_upstream_source_properties(intersect_output_source.get(),
                                                context->source_operator(ops_with_intersect_build_sink));
    operators_with_intersect_output_source.emplace_back(std::move(intersect_output_source));
    if (limit() != -1) {
        operators_with_intersect_output_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    return operators_with_intersect_output_source;
}

} // namespace starrocks
