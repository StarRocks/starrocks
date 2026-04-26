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

#include "exec/hash_join_node.h"

#include <glog/logging.h>
#include <runtime/runtime_state.h>

#include <memory>
#include <type_traits>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/runtime_profile.h"
#include "exec/hash_joiner.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/group_execution/execution_group_builder.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/hashjoin/hash_join_build_operator.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/hashjoin/spillable_hash_join_build_operator.h"
#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/pipeline/spill_process_operator.h"
#include "exec/runtime_filter/runtime_filter_descriptor.h"
#include "exec/runtime_filter/runtime_filter_probe.h"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "runtime/runtime_filter_worker.h"

namespace starrocks {

HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : PipelineNode(pool, tnode, descs),
          _hash_join_node(tnode.hash_join_node),
          _join_type(tnode.hash_join_node.join_op) {
    if (_join_type == TJoinOp::LEFT_ANTI_JOIN && tnode.hash_join_node.is_rewritten_from_not_in) {
        _join_type = TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    }
    if (tnode.hash_join_node.__isset.distribution_mode) {
        _distribution_mode = tnode.hash_join_node.distribution_mode;
    }
}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    if (tnode.hash_join_node.__isset.sql_join_predicates) {
        _runtime_profile->add_info_string("JoinPredicates", tnode.hash_join_node.sql_join_predicates);
    }
    if (tnode.hash_join_node.__isset.sql_predicates) {
        _runtime_profile->add_info_string("Predicates", tnode.hash_join_node.sql_predicates);
    }

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        ExprContext* left = nullptr;
        ExprContext* right = nullptr;
        RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, eq_join_conjunct.left, &left, state));
        _probe_expr_ctxs.push_back(left);
        RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, eq_join_conjunct.right, &right, state));
        _build_expr_ctxs.push_back(right);
        if (!left->root()->type().support_join() || !right->root()->type().support_join()) {
            return Status::NotSupported(fmt::format("join on type {}={} is not supported",
                                                    left->root()->type().debug_string(),
                                                    right->root()->type().debug_string()));
        }

        if (eq_join_conjunct.__isset.opcode && eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) {
            _is_null_safes.emplace_back(true);
        } else {
            _is_null_safes.emplace_back(false);
        }
    }

    if (tnode.hash_join_node.__isset.asof_join_condition) {
        auto asof_join_condition = tnode.hash_join_node.asof_join_condition;
        RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, asof_join_condition.left,
                                                      &_asof_join_condition_probe_expr_ctx, state));
        RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, asof_join_condition.right,
                                                      &_asof_join_condition_build_expr_ctx, state));
        _asof_join_condition_op = tnode.hash_join_node.asof_join_condition.opcode;
    }

    if (tnode.hash_join_node.__isset.partition_exprs) {
        // the same column can appear more than once in either lateral side of eq_join_conjuncts, but multiple
        // occurrences are accounted for once when determining local shuffle partition_exprs for bucket shuffle join.
        // for an example:
        // table t1 is bucketed by c1, the query 'select * from t1,t2 on t1.c1 = t2.c1 and t1.c1 in (TRUE, NULL) =
        // t2.c1', SlotRef(t2.c1) appears twice, but the local shuffle partition_exprs should have the same number of
        // exprs as partition_exprs used by ExchangeNode that sends data to right child of the HashJoin.
        for (const auto& partition_expr : tnode.hash_join_node.partition_exprs) {
            bool match_exactly_once = false;
            for (auto i = 0; i < eq_join_conjuncts.size(); ++i) {
                const auto& eq_join_conjunct = eq_join_conjuncts[i];
                if (eq_join_conjunct.left == partition_expr || eq_join_conjunct.right == partition_expr) {
                    match_exactly_once = true;
                    _probe_equivalence_partition_expr_ctxs.push_back(_probe_expr_ctxs[i]);
                    _build_equivalence_partition_expr_ctxs.push_back(_build_expr_ctxs[i]);
                    break;
                }
            }
            DCHECK(match_exactly_once);
        }
    } else {
        // partition_exprs is only avaiable for the bucket shuffle join,
        // so local shuffle use _probe_expr_ctxs and _build_expr_ctxs for the other joins.
        _probe_equivalence_partition_expr_ctxs = _probe_expr_ctxs;
        _build_equivalence_partition_expr_ctxs = _build_expr_ctxs;
    }

    if (tnode.__isset.hash_join_node && tnode.hash_join_node.__isset.common_slot_map) {
        for (const auto& [key, val] : tnode.hash_join_node.common_slot_map) {
            ExprContext* context;
            RETURN_IF_ERROR(ExprFactory::create_expr_tree(_pool, val, &context, state, true));
            _common_expr_ctxs.insert({key, context});
        }
    }

    RETURN_IF_ERROR(ExprFactory::create_expr_trees(_pool, tnode.hash_join_node.other_join_conjuncts,
                                                   &_other_join_conjunct_ctxs, state));

    for (const auto& desc : tnode.hash_join_node.build_runtime_filters) {
        auto* rf_desc = _pool->add(new RuntimeFilterBuildDescriptor());
        RETURN_IF_ERROR(rf_desc->init(_pool, desc, state));
        _build_runtime_filters.emplace_back(rf_desc);
    }
    _runtime_join_filter_pushdown_limit = 1024000;
    if (state->query_options().__isset.runtime_join_filter_pushdown_limit) {
        _runtime_join_filter_pushdown_limit = state->query_options().runtime_join_filter_pushdown_limit;
    }

    if (tnode.hash_join_node.__isset.output_columns) {
        _output_slots.insert(tnode.hash_join_node.output_columns.begin(), tnode.hash_join_node.output_columns.end());
    }
    if (tnode.hash_join_node.__isset.late_materialization) {
        _enable_late_materialization = tnode.hash_join_node.late_materialization;
    }

    if (tnode.hash_join_node.__isset.enable_partition_hash_join) {
        _enable_partition_hash_join = tnode.hash_join_node.enable_partition_hash_join;
    }

    if (tnode.hash_join_node.__isset.is_skew_join) {
        _is_skew_join = tnode.hash_join_node.is_skew_join;
    }
    return Status::OK();
}

void HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    ExprExecutor::close(_build_expr_ctxs, state);
    ExprExecutor::close(_probe_expr_ctxs, state);
    ExprExecutor::close(_other_join_conjunct_ctxs, state);
    if (_asof_join_condition_build_expr_ctx != nullptr) {
        _asof_join_condition_build_expr_ctx->close(state);
    }
    if (_asof_join_condition_probe_expr_ctx != nullptr) {
        _asof_join_condition_probe_expr_ctx->close(state);
    }

    _ht.close();

    ExecNode::close(state);
}

template <class HashJoinerFactory, class HashJoinBuilderFactory, class HashJoinProbeFactory>
StatusOr<pipeline::OpFactories> HashJoinNode::_decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    ASSIGN_OR_RETURN(auto rhs_operators, child(1)->decompose_to_pipeline(context));
    // "col NOT IN (NULL, val1, val2)" always returns false, so hash join should
    // return empty result in this case. Hash join cannot be divided into multiple
    // partitions in this case. Otherwise, NULL value in right table will only occur
    // in some partition hash table, and other partition hash table can output chunk.
    // TODO: support nullaware left anti join with shuffle join
    DCHECK(_join_type != TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN || _distribution_mode == TJoinDistributionMode::BROADCAST);
    if (_distribution_mode == TJoinDistributionMode::BROADCAST) {
        // Broadcast join need only create one hash table, because all the HashJoinProbeOperators
        // use the same hash table with their own different probe states.
        rhs_operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), rhs_operators);
    } else {
        // Both HashJoin{Build, Probe}Operator are parallelized
        // There are two ways of shuffle
        // 1. If previous op is ExchangeSourceOperator and its partition type is HASH_PARTITIONED or BUCKET_SHUFFLE_HASH_PARTITIONED
        // then pipeline level shuffle will be performed at sender side (ExchangeSinkOperator), so
        // there is no need to perform local shuffle again at receiver side
        // 2. Otherwise, add LocalExchangeOperator
        // to shuffle multi-stream into #degree_of_parallelism# streams each of that pipes into HashJoin{Build, Probe}Operator.
        rhs_operators = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), id(), rhs_operators,
                                                                          _build_equivalence_partition_expr_ctxs);
    }

    size_t num_right_partitions = context->source_operator(rhs_operators)->degree_of_parallelism();

    auto workgroup = context->fragment_context()->workgroup();
    auto build_side_spill_channel_factory = std::make_shared<SpillProcessChannelFactory>(num_right_partitions);

    if (runtime_state()->enable_spill() && runtime_state()->enable_hash_join_spill() &&
        std::is_same_v<HashJoinBuilderFactory, SpillableHashJoinBuildOperatorFactory>) {
        context->interpolate_spill_process(id(), build_side_spill_channel_factory, num_right_partitions);
    }

    auto* pool = context->fragment_context()->runtime_state()->obj_pool();
    HashJoinerParam param(pool, _hash_join_node, _is_null_safes, _build_expr_ctxs, _probe_expr_ctxs,
                          _other_join_conjunct_ctxs, _conjunct_ctxs, child(1)->row_desc(), child(0)->row_desc(),
                          child(1)->type(), child(0)->type(), child(1)->conjunct_ctxs().empty(), _build_runtime_filters,
                          _output_slots, _output_slots, context->degree_of_parallelism(), _distribution_mode,
                          _enable_late_materialization, _enable_partition_hash_join, _is_skew_join, _common_expr_ctxs,
                          _asof_join_condition_op, _asof_join_condition_probe_expr_ctx,
                          _asof_join_condition_build_expr_ctx);
    auto hash_joiner_factory = std::make_shared<starrocks::pipeline::HashJoinerFactory>(param);

    // Create a shared RefCountedRuntimeFilterCollector
    auto rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    // In default query engine, we only build one hash table for join right child.
    // But for pipeline query engine, we will build `num_right_partitions` hash tables, so we need to enlarge the limit

    const auto& query_options = runtime_state()->query_options();

    size_t global_runtime_filter_build_max_size = UINT64_MAX;
    if (query_options.__isset.global_runtime_filter_build_max_size &&
        query_options.global_runtime_filter_build_max_size > 0) {
        global_runtime_filter_build_max_size = query_options.global_runtime_filter_build_max_size;
    }

    size_t runtime_join_filter_pushdown_limit = UINT64_MAX;
    // _runtime_join_filter_pushdown_limit can be set by user, here we should prevent overflow
    if (_runtime_join_filter_pushdown_limit < UINT64_MAX / num_right_partitions) {
        runtime_join_filter_pushdown_limit = _runtime_join_filter_pushdown_limit * num_right_partitions;
    }

    auto partial_rf_merger = std::make_unique<PartialRuntimeFilterMerger>(
            pool, runtime_join_filter_pushdown_limit, global_runtime_filter_build_max_size,
            runtime_state()->func_version(), runtime_state()->enable_join_runtime_bitset_filter());

    auto build_op = std::make_shared<HashJoinBuilderFactory>(context->next_operator_id(), id(), hash_joiner_factory,
                                                             std::move(partial_rf_merger), _distribution_mode,
                                                             build_side_spill_channel_factory);
    pipeline::init_runtime_filter_for_operator(*this, build_op.get(), context, rc_rf_probe_collector);

    auto probe_op = std::make_shared<HashJoinProbeFactory>(context->next_operator_id(), id(), hash_joiner_factory);
    pipeline::init_runtime_filter_for_operator(*this, probe_op.get(), context, rc_rf_probe_collector);

    rhs_operators.emplace_back(std::move(build_op));
    context->add_pipeline(rhs_operators);
    context->push_dependent_pipeline(context->last_pipeline());
    DeferOp pop_dependent_pipeline([context]() { context->pop_dependent_pipeline(); });

    ASSIGN_OR_RETURN(auto lhs_operators, child(0)->decompose_to_pipeline(context));
    auto join_colocate_group = context->find_exec_group_by_plan_node_id(_id);
    if (join_colocate_group->type() == ExecutionGroupType::COLOCATE) {
        DCHECK(context->current_execution_group()->is_colocate_exec_group());
        DCHECK_EQ(context->current_execution_group(), join_colocate_group);
        context->set_current_execution_group(join_colocate_group);
    } else {
        // left child is colocate group, but current join is not colocate group
        if (context->current_execution_group()->is_colocate_exec_group()) {
            lhs_operators = context->interpolate_grouped_exchange(_id, lhs_operators);
        }

        if (_distribution_mode == TJoinDistributionMode::BROADCAST) {
            lhs_operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), lhs_operators,
                                                                                  context->degree_of_parallelism());
        } else {
            auto* rhs_source_op = context->source_operator(rhs_operators);
            auto* lhs_source_op = context->source_operator(lhs_operators);
            DCHECK_EQ(rhs_source_op->partition_type(), lhs_source_op->partition_type());
            lhs_operators = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), id(), lhs_operators,
                                                                              _probe_equivalence_partition_expr_ctxs);
        }
    }

    lhs_operators.emplace_back(std::move(probe_op));
    // add placeholder into RuntimeFilterHub, HashJoinBuildOperator will generate runtime filters and fill it,
    // Operators consuming the runtime filters will inspect this placeholder.
    if (context->is_colocate_group() && _distribution_mode == TJoinDistributionMode::COLOCATE) {
        for (auto runtime_filter_build_desc : _build_runtime_filters) {
            // local colocate won't generate global runtime filter
            DCHECK(!runtime_filter_build_desc->has_remote_targets());
            runtime_filter_build_desc->set_num_colocate_partition(num_right_partitions);
        }
        size_t num_left_partition = context->source_operator(lhs_operators)->degree_of_parallelism();
        DCHECK_EQ(num_left_partition, num_right_partitions);
        context->fragment_context()->runtime_filter_hub()->add_holder(_id, num_right_partitions);
    } else {
        context->fragment_context()->runtime_filter_hub()->add_holder(_id);
    }

    if (limit() != -1) {
        lhs_operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    if (_hash_join_node.__isset.interpolate_passthrough && _hash_join_node.interpolate_passthrough &&
        !context->is_colocate_group()) {
        lhs_operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), lhs_operators,
                                                                              context->degree_of_parallelism(), true);
    }

    // Use ChunkAccumulateOperator, when any following condition occurs:
    // - not left/asof left outer join,
    // - left outer join, with conjuncts or runtime filters.
    bool need_accumulate_chunk =
            (_join_type != TJoinOp::LEFT_OUTER_JOIN && _join_type != TJoinOp::ASOF_LEFT_OUTER_JOIN) ||
            !_conjunct_ctxs.empty() || !_other_join_conjunct_ctxs.empty() ||
            lhs_operators.back()->has_runtime_filters();
    if (need_accumulate_chunk) {
        pipeline::may_add_chunk_accumulate_operator(lhs_operators, context, id());
    }

    return lhs_operators;
}

StatusOr<pipeline::OpFactories> HashJoinNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    // now spill only support INNER_JOIN and LEFT-SEMI JOIN. we could implement LEFT_OUTER_JOIN later
    if (runtime_state()->enable_spill() && runtime_state()->enable_hash_join_spill() && is_spillable(_join_type)) {
        return _decompose_to_pipeline<HashJoinerFactory, SpillableHashJoinBuildOperatorFactory,
                                      SpillableHashJoinProbeOperatorFactory>(context);
    } else {
        return _decompose_to_pipeline<HashJoinerFactory, HashJoinBuildOperatorFactory, HashJoinProbeOperatorFactory>(
                context);
    }
}

bool HashJoinNode::can_generate_global_runtime_filter() const {
    return std::any_of(_build_runtime_filters.begin(), _build_runtime_filters.end(),
                       [](const RuntimeFilterBuildDescriptor* rf) { return rf->has_remote_targets(); });
}

void HashJoinNode::push_down_join_runtime_filter(RuntimeState* state, RuntimeFilterProbeCollector* collector) {
    if (collector->empty()) return;
    if (_join_type == TJoinOp::INNER_JOIN || _join_type == TJoinOp::LEFT_SEMI_JOIN ||
        _join_type == TJoinOp::RIGHT_SEMI_JOIN) {
        ExecNode::push_down_join_runtime_filter(state, collector);
        return;
    }
    _runtime_filter_collector.push_down(state, id(), collector, _tuple_ids, _local_rf_waiting_set);
}

TJoinDistributionMode::type HashJoinNode::distribution_mode() const {
    return _distribution_mode;
}

const std::list<RuntimeFilterBuildDescriptor*>& HashJoinNode::build_runtime_filters() const {
    return _build_runtime_filters;
}

} // namespace starrocks
