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

#include "exec/cross_join_node.h"

#include <memory>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/statusor.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/nljoin/nljoin_build_operator.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/nljoin/nljoin_probe_operator.h"
#include "exec/pipeline/nljoin/spillable_nljoin_build_operator.h"
#include "exec/pipeline/nljoin/spillable_nljoin_probe_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/runtime_filter/runtime_filter_helper.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "exprs/literal.h"
#include "gen_cpp/PlanNodes_types.h"
#include "glog/logging.h"
#include "runtime/runtime_state.h"

namespace starrocks {

CrossJoinNode::CrossJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs) {}

static bool _support_join_type(TJoinOp::type join_type) {
    // TODO: support all join types
    switch (join_type) {
    case TJoinOp::CROSS_JOIN:
    case TJoinOp::INNER_JOIN:
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::RIGHT_OUTER_JOIN:
    case TJoinOp::FULL_OUTER_JOIN:
    case TJoinOp::LEFT_SEMI_JOIN:
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
        return true;
    default:
        return false;
    }
}

Status CrossJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (tnode.__isset.nestloop_join_node) {
        _join_op = tnode.nestloop_join_node.join_op;
        if (!state->enable_pipeline_engine() && _join_op != TJoinOp::CROSS_JOIN && _join_op != TJoinOp::INNER_JOIN) {
            return Status::NotSupported("non-pipeline engine only support CROSS JOIN");
        }
        if (!_support_join_type(_join_op)) {
            std::string type_string = starrocks::to_string(_join_op);
            return Status::NotSupported("nest-loop join not support: " + type_string);
        }

        if (tnode.nestloop_join_node.__isset.join_conjuncts) {
            RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, tnode.nestloop_join_node.join_conjuncts,
                                                           &_join_conjuncts, state));
        }

        if (tnode.nestloop_join_node.__isset.interpolate_passthrough) {
            _interpolate_passthrough = tnode.nestloop_join_node.interpolate_passthrough;
        }
        if (tnode.nestloop_join_node.__isset.sql_join_conjuncts) {
            _sql_join_conjuncts = tnode.nestloop_join_node.sql_join_conjuncts;
        }
        if (tnode.nestloop_join_node.__isset.build_runtime_filters) {
            for (const auto& desc : tnode.nestloop_join_node.build_runtime_filters) {
                auto* rf_desc = _pool->add(new RuntimeFilterBuildDescriptor());
                RETURN_IF_ERROR(rf_desc->init(_pool, desc, state));
                _build_runtime_filters.emplace_back(rf_desc);
            }
        }
        if (tnode.nestloop_join_node.__isset.common_slot_map) {
            for (const auto& [key, val] : tnode.nestloop_join_node.common_slot_map) {
                ExprContext* context;
                RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, val, &context, state, true));
                _common_expr_ctxs.insert({key, context});
            }
        }
        return Status::OK();
    }

    for (const auto& desc : tnode.cross_join_node.build_runtime_filters) {
        auto* rf_desc = _pool->add(new RuntimeFilterBuildDescriptor());
        RETURN_IF_ERROR(rf_desc->init(_pool, desc, state));
        _build_runtime_filters.emplace_back(rf_desc);
    }
    DCHECK_LE(_build_runtime_filters.size(), _conjunct_ctxs.size());
    return Status::OK();
}

void CrossJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    ExprExecutor::close(_join_conjuncts, state);
    ExecNode::close(state);
}

StatusOr<std::list<ExprContext*>> CrossJoinNode::rewrite_runtime_filter(
        ObjectPool* pool, const std::vector<RuntimeFilterBuildDescriptor*>& rf_descs, Chunk* chunk,
        const std::vector<ExprContext*>& ctxs) {
    std::list<ExprContext*> filters;

    for (auto rf_desc : rf_descs) {
        DCHECK_LT(rf_desc->build_expr_order(), ctxs.size());
        ASSIGN_OR_RETURN(auto expr, RuntimeFilterHelper::rewrite_runtime_filter_in_cross_join_node(
                                            pool, ctxs[rf_desc->build_expr_order()], chunk))
        filters.push_back(expr);
    }
    return filters;
}

template <class BuildFactory, class ProbeFactory>
StatusOr<pipeline::OpFactories> CrossJoinNode::_decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    // step 0: construct pipeline end with cross join right operator.
    ASSIGN_OR_RETURN(auto right_ops, _children[1]->decompose_to_pipeline(context));

    // define a runtime filter holder
    context->fragment_context()->runtime_filter_hub()->add_holder(_id);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));

    // step 1: construct pipeline end with cross join left operator(cross join left maybe not sink operator).
    NLJoinContextParams context_params;
    context_params.plan_node_id = _id;
    context_params.rf_hub = context->fragment_context()->runtime_filter_hub();
    context_params.rf_descs = std::move(_build_runtime_filters);
    // The order or filters should keep same with NestLoopJoinNode::buildRuntimeFilters
    context_params.filters = _join_conjuncts;
    std::copy(conjunct_ctxs().begin(), conjunct_ctxs().end(), std::back_inserter(context_params.filters));

    size_t num_right_partitions = context->source_operator(right_ops)->degree_of_parallelism();
    auto workgroup = context->fragment_context()->workgroup();
    auto spill_process_factory_ptr = std::make_shared<SpillProcessChannelFactory>(num_right_partitions);
    context_params.spill_process_factory_ptr = spill_process_factory_ptr;

    auto cross_join_context = std::make_shared<NLJoinContext>(std::move(context_params));

    // cross_join_right as sink operator
    auto right_factory = std::make_shared<BuildFactory>(context->next_operator_id(), id(), cross_join_context);
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, right_factory.get(), context, rc_rf_probe_collector);

    right_ops.emplace_back(std::move(right_factory));
    context->add_pipeline(right_ops);
    context->push_dependent_pipeline(context->last_pipeline());
    DeferOp pop_dependent_pipeline([context]() { context->pop_dependent_pipeline(); });

    ASSIGN_OR_RETURN(auto left_ops, _children[0]->decompose_to_pipeline(context));
    // communication with CrossJoinRight through shared_data.
    auto left_factory = std::make_shared<ProbeFactory>(
            context->next_operator_id(), id(), _row_descriptor, child(0)->row_desc(), child(1)->row_desc(),
            _sql_join_conjuncts, std::move(_join_conjuncts), std::move(_conjunct_ctxs), std::move(_common_expr_ctxs),
            std::move(cross_join_context), _join_op);
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, left_factory.get(), context, rc_rf_probe_collector);
    if (!context->is_colocate_group()) {
        left_ops = context->maybe_interpolate_local_adpative_passthrough_exchange(runtime_state(), id(), left_ops,
                                                                                  context->degree_of_parallelism());
    }
    left_ops.emplace_back(std::move(left_factory));

    if (limit() != -1) {
        left_ops.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    if (_interpolate_passthrough && !context->is_colocate_group()) {
        left_ops = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), left_ops,
                                                                         context->degree_of_parallelism(), true);
    }

    if constexpr (std::is_same_v<BuildFactory, SpillableNLJoinBuildOperatorFactory>) {
        pipeline::may_add_chunk_accumulate_operator(left_ops, context, id());
    }

    // return as the following pipeline
    return left_ops;
}

StatusOr<pipeline::OpFactories> CrossJoinNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    if (runtime_state()->enable_spill() && runtime_state()->enable_nl_join_spill() && _join_op == TJoinOp::CROSS_JOIN) {
        return _decompose_to_pipeline<SpillableNLJoinBuildOperatorFactory, SpillableNLJoinProbeOperatorFactory>(
                context);
    } else {
        return _decompose_to_pipeline<NLJoinBuildOperatorFactory, NLJoinProbeOperatorFactory>(context);
    }
}

} // namespace starrocks
