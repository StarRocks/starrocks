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

#include <runtime/runtime_state.h>

#include <memory>
#include <type_traits>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/hash_joiner.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
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
#include "exprs/expr.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/runtime_filter_bank.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_filter_worker.h"
#include "simd/simd.h"
#include "util/runtime_profile.h"

namespace starrocks {

HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _hash_join_node(tnode.hash_join_node),
          _join_type(tnode.hash_join_node.join_op) {
    _is_push_down = tnode.hash_join_node.is_push_down;
    if (_join_type == TJoinOp::LEFT_ANTI_JOIN && tnode.hash_join_node.is_rewritten_from_not_in) {
        _join_type = TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    }
    _build_runtime_filters_from_planner = false;
    if (tnode.hash_join_node.__isset.build_runtime_filters_from_planner) {
        _build_runtime_filters_from_planner = tnode.hash_join_node.build_runtime_filters_from_planner;
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
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.left, &left, state));
        _probe_expr_ctxs.push_back(left);
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjunct.right, &right, state));
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

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.hash_join_node.other_join_conjuncts,
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
    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _build_timer = ADD_TIMER(_runtime_profile, "BuildTime");

    _copy_right_table_chunk_timer = ADD_CHILD_TIMER(_runtime_profile, "1-CopyRightTableChunkTime", "BuildTime");
    _build_ht_timer = ADD_CHILD_TIMER(_runtime_profile, "2-BuildHashTableTime", "BuildTime");
    _build_push_down_expr_timer = ADD_CHILD_TIMER(_runtime_profile, "3-BuildPushDownExprTime", "BuildTime");
    _build_conjunct_evaluate_timer = ADD_CHILD_TIMER(_runtime_profile, "4-BuildConjunctEvaluateTime", "BuildTime");

    _probe_timer = ADD_TIMER(_runtime_profile, "ProbeTime");
    _merge_input_chunk_timer = ADD_CHILD_TIMER(_runtime_profile, "1-MergeInputChunkTime", "ProbeTime");
    _search_ht_timer = ADD_CHILD_TIMER(_runtime_profile, "2-SearchHashTableTime", "ProbeTime");
    _output_build_column_timer = ADD_CHILD_TIMER(_runtime_profile, "3-OutputBuildColumnTime", "ProbeTime");
    _output_probe_column_timer = ADD_CHILD_TIMER(_runtime_profile, "4-OutputProbeColumnTime", "ProbeTime");
    _probe_conjunct_evaluate_timer = ADD_CHILD_TIMER(_runtime_profile, "5-ProbeConjunctEvaluateTime", "ProbeTime");
    _other_join_conjunct_evaluate_timer =
            ADD_CHILD_TIMER(_runtime_profile, "6-OtherJoinConjunctEvaluateTime", "ProbeTime");
    _where_conjunct_evaluate_timer = ADD_CHILD_TIMER(_runtime_profile, "7-WhereConjunctEvaluateTime", "ProbeTime");

    _probe_rows_counter = ADD_COUNTER(_runtime_profile, "ProbeRows", TUnit::UNIT);
    _build_rows_counter = ADD_COUNTER(_runtime_profile, "BuildRows", TUnit::UNIT);
    _build_buckets_counter = ADD_COUNTER(_runtime_profile, "BuildBuckets", TUnit::UNIT);
    _push_down_expr_num = ADD_COUNTER(_runtime_profile, "PushDownExprNum", TUnit::UNIT);
    _avg_input_probe_chunk_size = ADD_COUNTER(_runtime_profile, "AvgInputProbeChunkSize", TUnit::UNIT);
    _avg_output_chunk_size = ADD_COUNTER(_runtime_profile, "AvgOutputChunkSize", TUnit::UNIT);
    _runtime_profile->add_info_string("JoinType", to_string(_join_type));

    RETURN_IF_ERROR(Expr::prepare(_build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_other_join_conjunct_ctxs, state));

    HashTableParam param;
    _init_hash_table_param(&param);
    _ht.create(param);

    _probe_column_count = _ht.get_probe_column_count();
    _build_column_count = _ht.get_build_column_count();

    return Status::OK();
}

void HashJoinNode::_init_hash_table_param(HashTableParam* param) {
    param->with_other_conjunct = !_other_join_conjunct_ctxs.empty();
    param->join_type = _join_type;
    param->row_desc = &_row_descriptor;
    param->build_row_desc = &child(1)->row_desc();
    param->probe_row_desc = &child(0)->row_desc();
    param->search_ht_timer = _search_ht_timer;
    param->output_build_column_timer = _output_build_column_timer;
    param->output_probe_column_timer = _output_probe_column_timer;
    param->build_output_slots = _output_slots;
    param->probe_output_slots = _output_slots;

    std::set<SlotId> predicate_slots;
    for (ExprContext* expr_context : _conjunct_ctxs) {
        std::vector<SlotId> expr_slots;
        expr_context->root()->get_slot_ids(&expr_slots);
        predicate_slots.insert(expr_slots.begin(), expr_slots.end());
    }
    for (ExprContext* expr_context : _other_join_conjunct_ctxs) {
        std::vector<SlotId> expr_slots;
        expr_context->root()->get_slot_ids(&expr_slots);
        predicate_slots.insert(expr_slots.begin(), expr_slots.end());
    }
    param->predicate_slots = std::move(predicate_slots);

    for (auto i = 0; i < _build_expr_ctxs.size(); i++) {
        Expr* expr = _build_expr_ctxs[i]->root();
        if (expr->is_slotref()) {
            param->join_keys.emplace_back(JoinKeyDesc{&expr->type(), _is_null_safes[i], down_cast<ColumnRef*>(expr)});
        } else {
            param->join_keys.emplace_back(JoinKeyDesc{&expr->type(), _is_null_safes[i], nullptr});
        }
    }
}

Status HashJoinNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    ScopedTimer<MonotonicStopWatch> build_timer(_build_timer);
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_other_join_conjunct_ctxs, state));

    {
        build_timer.stop();
        RETURN_IF_ERROR(child(1)->open(state));
        build_timer.start();
    }

    while (true) {
        ChunkPtr chunk = nullptr;
        bool eos = false;
        {
            RETURN_IF_CANCELLED(state);
            // fetch chunk of right table
            build_timer.stop();
            RETURN_IF_ERROR(child(1)->get_next(state, &chunk, &eos));
            build_timer.start();
            if (eos) {
                break;
            }

            if (chunk->num_rows() <= 0) {
                continue;
            }
        }

        if (_ht.get_row_count() + chunk->num_rows() >= UINT32_MAX) {
            return Status::NotSupported(strings::Substitute("row count of right table in hash join > $0", UINT32_MAX));
        }

        {
            SCOPED_TIMER(_build_conjunct_evaluate_timer);
            RETURN_IF_ERROR(_evaluate_build_keys(chunk));
        }

        {
            // copy chunk of right table
            SCOPED_TIMER(_copy_right_table_chunk_timer);
            TRY_CATCH_BAD_ALLOC(_ht.append_chunk(chunk, _key_columns));
        }
    }

    {
        RETURN_IF_ERROR(_build(state));
        COUNTER_SET(_build_rows_counter, static_cast<int64_t>(_ht.get_row_count()));
        COUNTER_SET(_build_buckets_counter, static_cast<int64_t>(_ht.get_bucket_size()));
    }

    if (_is_push_down) {
        if (_children[0]->type() == TPlanNodeType::EXCHANGE_NODE &&
            _children[1]->type() == TPlanNodeType::EXCHANGE_NODE) {
            _is_push_down = false;
        } else if (_ht.get_row_count() > _runtime_join_filter_pushdown_limit) {
            _is_push_down = false;
        }

        if (_is_push_down || !child(1)->conjunct_ctxs().empty()) {
            // In filter could be used to fast compute segment row range in storage engine
            RETURN_IF_ERROR(_push_down_in_filter(state));
            RETURN_IF_ERROR(_create_implicit_local_join_runtime_filters(state));
        }
    }

    // it's quite critical to put publish runtime filters before short-circuit of
    // "inner-join with empty right table". because for global runtime filter
    // merge node is waiting for all partitioned runtime filter, so even hash row count is zero
    // we still have to build it.
    RETURN_IF_ERROR(_do_publish_runtime_filters(state, _runtime_join_filter_pushdown_limit));

    build_timer.stop();
    RETURN_IF_ERROR(child(0)->open(state));
    build_timer.start();

    // special cases of short-circuit break.
    if (_ht.get_row_count() == 0 && (_join_type == TJoinOp::INNER_JOIN || _join_type == TJoinOp::LEFT_SEMI_JOIN)) {
        _eos = true;
        return Status::OK();
    }

    if (_ht.get_row_count() > 0) {
        if (_join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN && _ht.get_key_columns().size() == 1 &&
            _has_null(_ht.get_key_columns()[0]) && _other_join_conjunct_ctxs.empty()) {
            // The current implementation of HashTable will reserve a row for judging the end of the linked list.
            // When performing expression calculations (such as cast string to int),
            // it is possible that this reserved row will generate Null,
            // so Column::has_null() cannot be used to judge whether there is Null in the right table.
            // TODO: This reserved field will be removed in the implementation mechanism in the future.
            // at that time, you can directly use Column::has_null() to judge
            _eos = true;
            return Status::OK();
        }
    }

    _mem_tracker->set(_ht.mem_usage());

    return Status::OK();
}

Status HashJoinNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    ScopedTimer<MonotonicStopWatch> probe_timer(_probe_timer);

    if (reached_limit()) {
        _eos = true;
        *eos = true;
        _final_update_profile();
        return Status::OK();
    }

    if (_eos) {
        *eos = true;
        _final_update_profile();
        return Status::OK();
    }

    *chunk = std::make_shared<Chunk>();

    bool tmp_eos = false;
    if (!_probe_eos || _ht_has_remain) {
        RETURN_IF_ERROR(_probe(state, probe_timer, chunk, tmp_eos));
        if (tmp_eos) {
            if (_join_type == TJoinOp::RIGHT_OUTER_JOIN || _join_type == TJoinOp::RIGHT_ANTI_JOIN ||
                _join_type == TJoinOp::FULL_OUTER_JOIN) {
                // fetch the remain data of hash table
                RETURN_IF_ERROR(_probe_remain(chunk, tmp_eos));
                if (tmp_eos) {
                    _eos = true;
                    *eos = true;
                    _final_update_profile();
                    return Status::OK();
                }
            } else {
                _eos = true;
                *eos = true;
                _final_update_profile();
                return Status::OK();
            }
        }
    } else {
        if (_right_table_has_remain) {
            if (_join_type == TJoinOp::RIGHT_OUTER_JOIN || _join_type == TJoinOp::RIGHT_ANTI_JOIN ||
                _join_type == TJoinOp::FULL_OUTER_JOIN) {
                // fetch the remain data of hash table
                RETURN_IF_ERROR(_probe_remain(chunk, tmp_eos));
                if (tmp_eos) {
                    _eos = true;
                    *eos = true;
                    _final_update_profile();
                    return Status::OK();
                }
            } else {
                _eos = true;
                *eos = true;
                _final_update_profile();
                return Status::OK();
            }
        } else {
            _eos = true;
            *eos = true;
            _final_update_profile();
            return Status::OK();
        }
    }

    DCHECK_LE((*chunk)->num_rows(), runtime_state()->chunk_size());
    _num_rows_returned += (*chunk)->num_rows();
    _output_chunk_count++;
    if (reached_limit()) {
        (*chunk)->set_num_rows((*chunk)->num_rows() - (_num_rows_returned - _limit));
        _num_rows_returned = _limit;
        COUNTER_SET(_rows_returned_counter, _limit);
    } else {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }

    DCHECK_EQ((*chunk)->num_columns(), (*chunk)->get_slot_id_to_index_map().size());

    *eos = false;
    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }

    Expr::close(_build_expr_ctxs, state);
    Expr::close(_probe_expr_ctxs, state);
    Expr::close(_other_join_conjunct_ctxs, state);

    _ht.close();

    ExecNode::close(state);
}

template <class HashJoinerFactory, class HashJoinBuilderFactory, class HashJoinProbeFactory>
pipeline::OpFactories HashJoinNode::_decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    auto rhs_operators = child(1)->decompose_to_pipeline(context);
    if (_distribution_mode == TJoinDistributionMode::BROADCAST) {
        // Broadcast join need only create one hash table, because all the HashJoinProbeOperators
        // use the same hash table with their own different probe states.
        rhs_operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), rhs_operators);
    } else {
        // "col NOT IN (NULL, val1, val2)" always returns false, so hash join should
        // return empty result in this case. Hash join cannot be divided into multiple
        // partitions in this case. Otherwise, NULL value in right table will only occur
        // in some partition hash table, and other partition hash table can output chunk.
        if (_join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
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
    }

    size_t num_right_partitions = context->source_operator(rhs_operators)->degree_of_parallelism();

    auto workgroup = context->fragment_context()->workgroup();
    auto executor = std::make_shared<spill::IOTaskExecutor>(
            ExecEnv::GetInstance()->scan_executor(), ExecEnv::GetInstance()->connector_scan_executor(), workgroup);
    auto build_side_spill_channel_factory =
            std::make_shared<SpillProcessChannelFactory>(num_right_partitions, std::move(executor));

    if (runtime_state()->enable_spill() && runtime_state()->enable_hash_join_spill() &&
        std::is_same_v<HashJoinBuilderFactory, SpillableHashJoinBuildOperatorFactory>) {
        context->interpolate_spill_process(id(), build_side_spill_channel_factory, num_right_partitions);
    }

    auto* pool = context->fragment_context()->runtime_state()->obj_pool();
    HashJoinerParam param(pool, _hash_join_node, _id, _type, _is_null_safes, _build_expr_ctxs, _probe_expr_ctxs,
                          _other_join_conjunct_ctxs, _conjunct_ctxs, child(1)->row_desc(), child(0)->row_desc(),
                          _row_descriptor, child(1)->type(), child(0)->type(), child(1)->conjunct_ctxs().empty(),
                          _build_runtime_filters, _output_slots, _output_slots, _distribution_mode, false);
    auto hash_joiner_factory = std::make_shared<starrocks::pipeline::HashJoinerFactory>(param);

    // add placeholder into RuntimeFilterHub, HashJoinBuildOperator will generate runtime filters and fill it,
    // Operators consuming the runtime filters will inspect this placeholder.
    context->fragment_context()->runtime_filter_hub()->add_holder(_id);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
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

    std::unique_ptr<PartialRuntimeFilterMerger> partial_rf_merger = std::make_unique<PartialRuntimeFilterMerger>(
            pool, runtime_join_filter_pushdown_limit, global_runtime_filter_build_max_size);

    auto build_op = std::make_shared<HashJoinBuilderFactory>(context->next_operator_id(), id(), hash_joiner_factory,
                                                             std::move(partial_rf_merger), _distribution_mode,
                                                             build_side_spill_channel_factory);
    this->init_runtime_filter_for_operator(build_op.get(), context, rc_rf_probe_collector);

    auto probe_op = std::make_shared<HashJoinProbeFactory>(context->next_operator_id(), id(), hash_joiner_factory);
    this->init_runtime_filter_for_operator(probe_op.get(), context, rc_rf_probe_collector);

    rhs_operators.emplace_back(std::move(build_op));
    context->add_pipeline(rhs_operators);
    context->push_dependent_pipeline(context->last_pipeline());
    DeferOp pop_dependent_pipeline([context]() { context->pop_dependent_pipeline(); });

    auto lhs_operators = child(0)->decompose_to_pipeline(context);
    if (_distribution_mode == TJoinDistributionMode::BROADCAST) {
        lhs_operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), lhs_operators,
                                                                              context->degree_of_parallelism());
    } else {
        if (_join_type == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            lhs_operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), lhs_operators);
        } else {
            auto* rhs_source_op = context->source_operator(rhs_operators);
            auto* lhs_source_op = context->source_operator(lhs_operators);
            DCHECK_EQ(rhs_source_op->partition_type(), lhs_source_op->partition_type());
            lhs_operators = context->maybe_interpolate_local_shuffle_exchange(runtime_state(), id(), lhs_operators,
                                                                              _probe_equivalence_partition_expr_ctxs);
        }
    }
    lhs_operators.emplace_back(std::move(probe_op));

    if (limit() != -1) {
        lhs_operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    if (_hash_join_node.__isset.interpolate_passthrough && _hash_join_node.interpolate_passthrough) {
        lhs_operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), lhs_operators,
                                                                              context->degree_of_parallelism(), true);
    }

    // Use ChunkAccumulateOperator, when any following condition occurs:
    // - not left outer join,
    // - left outer join, with conjuncts or runtime filters.
    bool need_accumulate_chunk = _join_type != TJoinOp::LEFT_OUTER_JOIN || !_conjunct_ctxs.empty() ||
                                 !_other_join_conjunct_ctxs.empty() || lhs_operators.back()->has_runtime_filters();
    if (need_accumulate_chunk) {
        may_add_chunk_accumulate_operator(lhs_operators, context, id());
    }

    return lhs_operators;
}

pipeline::OpFactories HashJoinNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
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

bool HashJoinNode::_has_null(const ColumnPtr& column) {
    if (column->is_nullable()) {
        const auto& null_column = ColumnHelper::as_raw_column<NullableColumn>(column)->null_column();
        DCHECK_GT(null_column->size(), 0);
        return null_column->contain_value(1, null_column->size(), 1);
    }
    return false;
}

Status HashJoinNode::_build(RuntimeState* state) {
    // build hash table
    SCOPED_TIMER(_build_ht_timer);
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_ht.build(state)));
    return Status::OK();
}

static inline bool check_chunk_zero_and_create_new(ChunkPtr* chunk) {
    if ((*chunk)->num_rows() <= 0) {
        // TODO: It's better to reuse the chunk object.
        // Use a new chunk to continue call _ht.probe.
        *chunk = std::make_shared<Chunk>();
        return true;
    }
    return false;
}

Status HashJoinNode::_evaluate_build_keys(const ChunkPtr& chunk) {
    _key_columns.resize(0);
    size_t num_rows = chunk->num_rows();
    for (auto& ctx : _build_expr_ctxs) {
        const TypeDescriptor& data_type = ctx->root()->type();
        ASSIGN_OR_RETURN(ColumnPtr key_column, ctx->evaluate(chunk.get()));
        if (key_column->only_null()) {
            ColumnPtr column = ColumnHelper::create_column(data_type, true);
            column->append_nulls(num_rows);
            _key_columns.emplace_back(column);
        } else if (key_column->is_constant()) {
            auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(key_column);
            const_column->data_column()->assign(num_rows, 0);
            _key_columns.emplace_back(const_column->data_column());
        } else {
            _key_columns.emplace_back(key_column);
        }
    }
    return Status::OK();
}

Status HashJoinNode::_probe(RuntimeState* state, ScopedTimer<MonotonicStopWatch>& probe_timer, ChunkPtr* chunk,
                            bool& eos) {
    while (true) {
        if (!_ht_has_remain) {
            while (true) {
                {
                    // if current chunk size >= vector_chunk_size / 2, direct return the current chunk
                    // if current chunk size < vector_chunk_size and pre chunk size + cur chunk size <= 1024, merge the two chunk
                    // if current chunk size < vector_chunk_size and pre chunk size + cur chunk size > 1024, return pre chunk
                    probe_timer.stop();
                    RETURN_IF_ERROR(child(0)->get_next(state, &_cur_left_input_chunk, &_probe_eos));
                    probe_timer.start();
                    {
                        SCOPED_TIMER(_merge_input_chunk_timer);
                        _probe_chunk_count++;
                        if (_probe_eos) {
                            if (_pre_left_input_chunk != nullptr) {
                                // has reserved probe chunk
                                eos = false;
                                _probing_chunk = std::move(_pre_left_input_chunk);
                            } else {
                                eos = true;
                                return Status::OK();
                            }
                        } else {
                            if (_cur_left_input_chunk->num_rows() <= 0) {
                                continue;
                            } else if (_cur_left_input_chunk->num_rows() >= runtime_state()->chunk_size() / 2) {
                                // the probe chunk size of read from right child >= runtime_state()->chunk_size(), direct return
                                _probing_chunk = std::move(_cur_left_input_chunk);
                            } else if (_pre_left_input_chunk == nullptr) {
                                // the probe chunk size is small, reserve for merge next probe chunk
                                _pre_left_input_chunk = std::move(_cur_left_input_chunk);
                                continue;
                            } else {
                                if (_cur_left_input_chunk->num_rows() + _pre_left_input_chunk->num_rows() >
                                    runtime_state()->chunk_size()) {
                                    // the two chunk size > runtime_state()->chunk_size(), return the first reserved chunk
                                    _probing_chunk = std::move(_pre_left_input_chunk);
                                    _pre_left_input_chunk = std::move(_cur_left_input_chunk);
                                } else {
                                    // TODO: copy the small chunk to big chunk
                                    Columns& dest_columns = _pre_left_input_chunk->columns();
                                    Columns& src_columns = _cur_left_input_chunk->columns();
                                    size_t num_rows = _cur_left_input_chunk->num_rows();
                                    // copy the new read chunk to the reserved
                                    for (size_t i = 0; i < dest_columns.size(); i++) {
                                        dest_columns[i]->append(*src_columns[i], 0, num_rows);
                                    }
                                    _cur_left_input_chunk.reset();
                                    continue;
                                }
                            }
                        }
                    }
                }

                COUNTER_UPDATE(_probe_rows_counter, _probing_chunk->num_rows());

                {
                    SCOPED_TIMER(_probe_conjunct_evaluate_timer);
                    _key_columns.resize(0);
                    for (auto& probe_expr_ctx : _probe_expr_ctxs) {
                        ASSIGN_OR_RETURN(ColumnPtr column_ptr, probe_expr_ctx->evaluate(_probing_chunk.get()));
                        if (column_ptr->is_nullable() && column_ptr->is_constant()) {
                            ColumnPtr column = ColumnHelper::create_column(probe_expr_ctx->root()->type(), true);
                            column->append_nulls(_probing_chunk->num_rows());
                            _key_columns.emplace_back(column);
                        } else if (column_ptr->is_constant()) {
                            auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(column_ptr);
                            const_column->data_column()->assign(_probing_chunk->num_rows(), 0);
                            _key_columns.emplace_back(const_column->data_column());
                        } else {
                            _key_columns.emplace_back(column_ptr);
                        }
                    }
                }

                DCHECK_GT(_key_columns.size(), 0);
                DCHECK(_key_columns[0].get() != nullptr);
                if (!_key_columns[0]->empty()) {
                    break;
                }
            }
        }

        TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_ht.probe(state, _key_columns, &_probing_chunk, chunk, &_ht_has_remain)));
        if (!_ht_has_remain) {
            _probing_chunk = nullptr;
        }

        eval_join_runtime_filters(chunk);

        if (check_chunk_zero_and_create_new(chunk)) {
            continue;
        }

        if (!_other_join_conjunct_ctxs.empty()) {
            SCOPED_TIMER(_other_join_conjunct_evaluate_timer);
            RETURN_IF_ERROR(_process_other_conjunct(chunk));
            if (check_chunk_zero_and_create_new(chunk)) {
                continue;
            }
        }

        if (!_conjunct_ctxs.empty()) {
            SCOPED_TIMER(_where_conjunct_evaluate_timer);
            RETURN_IF_ERROR(eval_conjuncts(_conjunct_ctxs, (*chunk).get()));

            if (check_chunk_zero_and_create_new(chunk)) {
                continue;
            }
        }

        break;
    }

    return Status::OK();
}

Status HashJoinNode::_probe_remain(ChunkPtr* chunk, bool& eos) {
    ScopedTimer<MonotonicStopWatch> probe_timer(_probe_timer);

    while (_right_table_has_remain) {
        TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_ht.probe_remain(runtime_state(), chunk, &_right_table_has_remain)));

        eval_join_runtime_filters(chunk);
        if (!_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(eval_conjuncts(_conjunct_ctxs, (*chunk).get()));
        }

        if (check_chunk_zero_and_create_new(chunk)) {
            continue;
        }

        eos = false;
        return Status::OK();
    }

    eos = true;
    return Status::OK();
}

Status HashJoinNode::_calc_filter_for_other_conjunct(ChunkPtr* chunk, Filter& filter, bool& filter_all, bool& hit_all) {
    filter_all = false;
    hit_all = false;
    filter.assign((*chunk)->num_rows(), 1);

    for (auto* ctx : _other_join_conjunct_ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate((*chunk).get()));
        size_t true_count = ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            filter_all = true;
            filter.assign((*chunk)->num_rows(), 0);
            break;
        } else {
            bool all_zero = false;
            ColumnHelper::merge_two_filters(column, &filter, &all_zero);
            if (all_zero) {
                filter_all = true;
                break;
            }
        }
    }

    if (!filter_all) {
        int zero_count = SIMD::count_zero(filter.data(), filter.size());
        if (zero_count == 0) {
            hit_all = true;
        }
    }

    return Status::OK();
}

void HashJoinNode::_process_row_for_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                   bool filter_all, bool hit_all, const Filter& filter) {
    if (filter_all) {
        for (size_t i = start_column; i < start_column + column_count; i++) {
            auto* null_column = ColumnHelper::as_raw_column<NullableColumn>((*chunk)->columns()[i]);
            auto& null_data = null_column->mutable_null_column()->get_data();
            for (size_t j = 0; j < (*chunk)->num_rows(); j++) {
                null_data[j] = 1;
                null_column->set_has_null(true);
            }
        }
    } else {
        if (hit_all) {
            return;
        }

        for (size_t i = start_column; i < start_column + column_count; i++) {
            auto* null_column = ColumnHelper::as_raw_column<NullableColumn>((*chunk)->columns()[i]);
            auto& null_data = null_column->mutable_null_column()->get_data();
            for (size_t j = 0; j < filter.size(); j++) {
                if (filter[j] == 0) {
                    null_data[j] = 1;
                    null_column->set_has_null(true);
                }
            }
        }
    }
}

Status HashJoinNode::_process_outer_join_with_other_conjunct(ChunkPtr* chunk, size_t start_column,
                                                             size_t column_count) {
    bool filter_all = false;
    bool hit_all = false;
    Filter filter;

    RETURN_IF_ERROR(_calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all));
    _process_row_for_other_conjunct(chunk, start_column, column_count, filter_all, hit_all, filter);

    _ht.remove_duplicate_index(&filter);
    (*chunk)->filter(filter);

    return Status::OK();
}

Status HashJoinNode::_process_semi_join_with_other_conjunct(ChunkPtr* chunk) {
    bool filter_all = false;
    bool hit_all = false;
    Filter filter;

    RETURN_IF_ERROR(_calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all));

    _ht.remove_duplicate_index(&filter);
    (*chunk)->filter(filter);

    return Status::OK();
}

Status HashJoinNode::_process_right_anti_join_with_other_conjunct(ChunkPtr* chunk) {
    bool filter_all = false;
    bool hit_all = false;
    Filter filter;

    RETURN_IF_ERROR(_calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all));

    _ht.remove_duplicate_index(&filter);
    (*chunk)->set_num_rows(0);

    return Status::OK();
}

Status HashJoinNode::_process_other_conjunct(ChunkPtr* chunk) {
    switch (_join_type) {
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::FULL_OUTER_JOIN:
        return _process_outer_join_with_other_conjunct(chunk, _probe_column_count, _build_column_count);
    case TJoinOp::RIGHT_OUTER_JOIN:
    case TJoinOp::LEFT_SEMI_JOIN:
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
    case TJoinOp::RIGHT_SEMI_JOIN:
        return _process_semi_join_with_other_conjunct(chunk);
    case TJoinOp::RIGHT_ANTI_JOIN:
        return _process_right_anti_join_with_other_conjunct(chunk);
    default:
        // the other join conjunct for inner join will be convert to other predicate
        // so can't reach here
        return eval_conjuncts(_other_join_conjunct_ctxs, (*chunk).get());
    }
}

Status HashJoinNode::_push_down_in_filter(RuntimeState* state) {
    SCOPED_TIMER(_build_push_down_expr_timer);

    if (_ht.get_row_count() > 1024) {
        return Status::OK();
    }

    if (_ht.get_row_count() > 0) {
        // there is a bug (DSDB-3860) in old planner if probe_expr is not slot-ref, and this fix is workaround.
        size_t size = _build_expr_ctxs.size();
        std::vector<bool> to_build(size, true);
        for (int i = 0; i < size; i++) {
            ExprContext* expr_ctx = _probe_expr_ctxs[i];
            to_build[i] = (expr_ctx->root()->is_slotref());
        }

        for (size_t i = 0; i < size; i++) {
            if (!to_build[i]) continue;
            ColumnPtr column = _ht.get_key_columns()[i];
            Expr* probe_expr = _probe_expr_ctxs[i]->root();
            // create and fill runtime IN filter.
            VectorizedInConstPredicateBuilder builder(state, _pool, probe_expr);
            builder.set_eq_null(_is_null_safes[i]);
            builder.use_as_join_runtime_filter();
            Status st = builder.create();
            if (!st.ok()) continue;
            builder.add_values(column, kHashJoinKeyColumnOffset);
            _runtime_in_filters.push_back(builder.get_in_const_predicate());
        }
    }

    if (_runtime_in_filters.empty()) {
        return Status::OK();
    }

    COUNTER_UPDATE(_push_down_expr_num, static_cast<int64_t>(_runtime_in_filters.size()));
    push_down_predicate(state, &_runtime_in_filters);

    return Status::OK();
}

Status HashJoinNode::_do_publish_runtime_filters(RuntimeState* state, int64_t limit) {
    SCOPED_TIMER(_build_push_down_expr_timer);

    // we build it even if hash table row count is 0
    // because for global runtime filter, we have to send that.
    for (auto* rf_desc : _build_runtime_filters) {
        // skip if it does not have consumer.
        if (!rf_desc->has_consumer()) continue;
        // skip if ht.size() > limit and it's only for local.
        if (!rf_desc->has_remote_targets() && _ht.get_row_count() > limit) continue;
        LogicalType build_type = rf_desc->build_expr_type();
        JoinRuntimeFilter* filter = RuntimeFilterHelper::create_runtime_bloom_filter(_pool, build_type);
        if (filter == nullptr) continue;
        filter->set_join_mode(rf_desc->join_mode());
        filter->init(_ht.get_row_count());
        int expr_order = rf_desc->build_expr_order();
        ColumnPtr column = _ht.get_key_columns()[expr_order];
        bool eq_null = _is_null_safes[expr_order];
        RETURN_IF_ERROR(RuntimeFilterHelper::fill_runtime_bloom_filter(column, build_type, filter,
                                                                       kHashJoinKeyColumnOffset, eq_null));
        rf_desc->set_runtime_filter(filter);
    }

    // publish runtime filters
    state->runtime_filter_port()->publish_runtime_filters(_build_runtime_filters);
    COUNTER_UPDATE(_push_down_expr_num, static_cast<int64_t>(_build_runtime_filters.size()));
    return Status::OK();
}

Status HashJoinNode::_create_implicit_local_join_runtime_filters(RuntimeState* state) {
    if (_build_runtime_filters_from_planner) return Status::OK();
    VLOG_FILE << "create implicit local join runtime filters";

    // to avoid filter id collision between multiple hash join nodes.
    const int implicit_runtime_filter_id_offset = 1000000 * (_id + 1);

    // build publish side.
    for (int i = 0; i < _build_expr_ctxs.size(); i++) {
        auto* desc = _pool->add(new RuntimeFilterBuildDescriptor());
        desc->_filter_id = implicit_runtime_filter_id_offset + i;
        desc->_build_expr_ctx = _build_expr_ctxs[i];
        desc->_build_expr_order = i;
        desc->_has_remote_targets = false;
        desc->_has_consumer = true;
        _build_runtime_filters.push_back(desc);
    }

    // build consume side.
    for (int i = 0; i < _probe_expr_ctxs.size(); i++) {
        auto* desc = _pool->add(new RuntimeFilterProbeDescriptor());
        desc->_filter_id = implicit_runtime_filter_id_offset + i;
        RETURN_IF_ERROR(_probe_expr_ctxs[i]->clone(state, _pool, &desc->_probe_expr_ctx));
        desc->_runtime_filter.store(nullptr);
        child(0)->register_runtime_filter_descriptor(state, desc);
    }

    // there are some runtime filters at child(0), try to push down them.
    child(0)->push_down_join_runtime_filter(state, &(child(0)->runtime_filter_collector()));

    return Status::OK();
}

} // namespace starrocks
