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

#include "exec/analytic_node.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include "column/chunk.h"
#include "exec/pipeline/analysis/analytic_sink_operator.h"
#include "exec/pipeline/analysis/analytic_source_operator.h"
#include "exec/pipeline/hash_partition_context.h"
#include "exec/pipeline/hash_partition_sink_operator.h"
#include "exec/pipeline/hash_partition_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exprs/agg/count.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

AnalyticNode::AnalyticNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _tnode(tnode),
          _result_tuple_desc(descs.get_tuple_descriptor(tnode.analytic_node.output_tuple_id)) {}

Status AnalyticNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _use_hash_based_partition = _tnode.analytic_node.order_by_exprs.empty() &&
                                _tnode.analytic_node.__isset.use_hash_based_partition &&
                                _tnode.analytic_node.use_hash_based_partition;

    if (!tnode.analytic_node.partition_exprs.empty()) {
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.analytic_node.partition_exprs, &_partition_exprs, state));
    }
    DCHECK(_conjunct_ctxs.empty());

    return Status::OK();
}

Status AnalyticNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK(child(0)->row_desc().is_prefix_of(row_desc()));

    _analytor = std::make_shared<Analytor>(_tnode, child(0)->row_desc(), _result_tuple_desc, false);
    RETURN_IF_ERROR(_analytor->prepare(state, _pool, runtime_profile()));

    return Status::OK();
}

Status AnalyticNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));

    return _analytor->open(state);
}

Status AnalyticNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    return Status::InternalError("should not call get_next() in AnalyticNode");
}

void AnalyticNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    if (_analytor != nullptr) {
        _analytor->close(state);
        _analytor.reset();
    }

    ExecNode::close(state);
}

pipeline::OpFactories AnalyticNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);
    auto* upstream_source_op = context->source_operator(ops_with_sink);
    bool is_skewed = _tnode.analytic_node.__isset.is_skewed && _tnode.analytic_node.is_skewed;

    if (_tnode.analytic_node.partition_exprs.empty()) {
        // analytic's dop must be 1 if with no partition clause
        ops_with_sink = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), ops_with_sink);
    } else if (_use_hash_based_partition) {
        // analytic has only partition by columns but no order by columns
        HashPartitionContextFactoryPtr hash_partition_ctx_factory =
                std::make_shared<HashPartitionContextFactory>(_tnode.analytic_node.partition_exprs);

        // prepend local shuffle to PartitionSortSinkOperator
        ops_with_sink = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), id(), ops_with_sink,
                                                                          _partition_exprs);
        upstream_source_op = context->source_operator(ops_with_sink);

        ops_with_sink.emplace_back(std::make_shared<HashPartitionSinkOperatorFactory>(context->next_operator_id(), id(),
                                                                                      hash_partition_ctx_factory));
        context->add_pipeline(ops_with_sink);

        ops_with_sink.clear();
        auto hash_partition_source_op = std::make_shared<HashPartitionSourceOperatorFactory>(
                context->next_operator_id(), id(), hash_partition_ctx_factory);
        context->inherit_upstream_source_properties(hash_partition_source_op.get(), upstream_source_op);
        ops_with_sink.push_back(std::move(hash_partition_source_op));
    } else if (is_skewed) {
        // The former sort will use passthrough exchange, so we need to add ordered partition local exchange here.
        ops_with_sink = context->maybe_interpolate_local_ordered_partition_exchange(runtime_state(), id(),
                                                                                    ops_with_sink, _partition_exprs);
    }

    upstream_source_op = context->source_operator(ops_with_sink);
    auto degree_of_parallelism = upstream_source_op->degree_of_parallelism();

    AnalytorFactoryPtr analytor_factory = std::make_shared<AnalytorFactory>(
            degree_of_parallelism, _tnode, child(0)->row_desc(), _result_tuple_desc, _use_hash_based_partition);
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));

    ops_with_sink.emplace_back(
            std::make_shared<AnalyticSinkOperatorFactory>(context->next_operator_id(), id(), _tnode, analytor_factory));
    this->init_runtime_filter_for_operator(ops_with_sink.back().get(), context, rc_rf_probe_collector);
    context->add_pipeline(ops_with_sink);

    OpFactories ops_with_source;
    auto source_op =
            std::make_shared<AnalyticSourceOperatorFactory>(context->next_operator_id(), id(), analytor_factory);
    source_op->set_skewed(is_skewed);
    this->init_runtime_filter_for_operator(source_op.get(), context, rc_rf_probe_collector);
    context->inherit_upstream_source_properties(source_op.get(), upstream_source_op);
    ops_with_source.push_back(std::move(source_op));

    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    return ops_with_source;
}

} // namespace starrocks
