// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/hash_joiner.h"

#include <runtime/runtime_state.h>

#include <memory>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"
#include "exprs/vectorized/in_const_predicate.hpp"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_filter_worker.h"
#include "simd/simd.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

HashJoiner::HashJoiner(const HashJoinerParam& param, const std::vector<HashJoinerPtr>& read_only_join_probers)
        : _hash_join_node(param._hash_join_node),
          _pool(param._pool),
          _join_type(param._hash_join_node.join_op),
          _is_null_safes(param._is_null_safes),
          _build_expr_ctxs(param._build_expr_ctxs),
          _probe_expr_ctxs(param._probe_expr_ctxs),
          _other_join_conjunct_ctxs(param._other_join_conjunct_ctxs),
          _conjunct_ctxs(param._conjunct_ctxs),
          _build_row_descriptor(param._build_row_descriptor),
          _probe_row_descriptor(param._probe_row_descriptor),
          _row_descriptor(param._row_descriptor),
          _build_node_type(param._build_node_type),
          _probe_node_type(param._probe_node_type),
          _build_conjunct_ctxs_is_empty(param._build_conjunct_ctxs_is_empty),
          _output_slots(param._output_slots),
          _build_runtime_filters(param._build_runtime_filters.begin(), param._build_runtime_filters.end()),
          _is_buildable(param._is_buildable),
          _read_only_join_probers(read_only_join_probers) {
    _is_push_down = param._hash_join_node.is_push_down;
    if (_join_type == TJoinOp::LEFT_ANTI_JOIN && param._hash_join_node.is_rewritten_from_not_in) {
        _join_type = TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;
    }

    _build_runtime_filters_from_planner = false;
    if (param._hash_join_node.__isset.build_runtime_filters_from_planner) {
        _build_runtime_filters_from_planner = param._hash_join_node.build_runtime_filters_from_planner;
    }
}

Status HashJoiner::prepare_builder(RuntimeState* state, RuntimeProfile* runtime_profile) {
    DCHECK(_is_buildable);

    if (_runtime_state == nullptr) {
        _runtime_state = state;
    }

    if (_hash_join_node.__isset.sql_join_predicates) {
        runtime_profile->add_info_string("JoinPredicates", _hash_join_node.sql_join_predicates);
    }
    if (_hash_join_node.__isset.sql_predicates) {
        runtime_profile->add_info_string("Predicates", _hash_join_node.sql_predicates);
    }

    runtime_profile->add_info_string("DistributionMode", to_string(_hash_join_node.distribution_mode));
    runtime_profile->add_info_string("JoinType", to_string(_join_type));
    _copy_right_table_chunk_timer = ADD_TIMER(runtime_profile, "CopyRightTableChunkTime");
    _build_ht_timer = ADD_TIMER(runtime_profile, "BuildHashTableTime");
    _build_runtime_filter_timer = ADD_TIMER(runtime_profile, "RuntimeFilterBuildTime");
    _build_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "BuildConjunctEvaluateTime");
    _build_buckets_counter = ADD_COUNTER(runtime_profile, "BuildBuckets", TUnit::UNIT);
    _runtime_filter_num = ADD_COUNTER(runtime_profile, "RuntimeFilterNum", TUnit::UNIT);

    HashTableParam param;
    _init_hash_table_param(&param);
    _ht.create(param);

    _probe_column_count = _ht.get_probe_column_count();
    _build_column_count = _ht.get_build_column_count();

    // The join builder also corresponds to a join prober.
    _num_unfinished_probers.store(_read_only_join_probers.size() + 1, std::memory_order_release);

    return Status::OK();
}

Status HashJoiner::prepare_prober(RuntimeState* state, RuntimeProfile* runtime_profile) {
    if (_runtime_state == nullptr) {
        _runtime_state = state;
    }

    runtime_profile->add_info_string("DistributionMode", to_string(_hash_join_node.distribution_mode));
    runtime_profile->add_info_string("JoinType", to_string(_join_type));
    _search_ht_timer = ADD_TIMER(runtime_profile, "SearchHashTableTime");
    _output_build_column_timer = ADD_TIMER(runtime_profile, "OutputBuildColumnTime");
    _output_probe_column_timer = ADD_TIMER(runtime_profile, "OutputProbeColumnTime");
    _output_tuple_column_timer = ADD_TIMER(runtime_profile, "OutputTupleColumnTime");
    _probe_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "ProbeConjunctEvaluateTime");
    _other_join_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "OtherJoinConjunctEvaluateTime");
    _where_conjunct_evaluate_timer = ADD_TIMER(runtime_profile, "WhereConjunctEvaluateTime");

    return Status::OK();
}

void HashJoiner::_init_hash_table_param(HashTableParam* param) {
    // Pipeline query engine always needn't create tuple columns
    param->need_create_tuple_columns = false;
    param->with_other_conjunct = !_other_join_conjunct_ctxs.empty();
    param->join_type = _join_type;
    param->row_desc = &_row_descriptor;
    param->build_row_desc = &_build_row_descriptor;
    param->probe_row_desc = &_probe_row_descriptor;
    param->search_ht_timer = _search_ht_timer;
    param->output_build_column_timer = _output_build_column_timer;
    param->output_probe_column_timer = _output_probe_column_timer;
    param->output_tuple_column_timer = _output_tuple_column_timer;

    param->output_slots = _output_slots;
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
    param->predicate_slots = predicate_slots;

    for (auto i = 0; i < _probe_expr_ctxs.size(); i++) {
        param->join_keys.emplace_back(JoinKeyDesc{_probe_expr_ctxs[i]->root()->type().type, _is_null_safes[i]});
    }
}
Status HashJoiner::append_chunk_to_ht(RuntimeState* state, const ChunkPtr& chunk) {
    if (_phase != HashJoinPhase::BUILD) {
        return Status::OK();
    }
    if (!chunk || chunk->is_empty()) {
        return Status::OK();
    }
    if (UNLIKELY(_ht.get_row_count() + chunk->num_rows() >= UINT32_MAX)) {
        return Status::NotSupported(strings::Substitute("row count of right table in hash join > $0", UINT32_MAX));
    }
    {
        // copy chunk of right table
        SCOPED_TIMER(_copy_right_table_chunk_timer);
        TRY_CATCH_BAD_ALLOC(_ht.append_chunk(state, chunk));
    }
    return Status::OK();
}

Status HashJoiner::build_ht(RuntimeState* state) {
    if (_phase == HashJoinPhase::BUILD) {
        RETURN_IF_ERROR(_build(state));
        COUNTER_SET(_build_buckets_counter, static_cast<int64_t>(_ht.get_bucket_size()));
    }

    return Status::OK();
}

bool HashJoiner::need_input() const {
    // when _buffered_chunk accumulates several chunks to form into a large enough chunk, it is moved into
    // _probe_chunk for probe operations.
    return _phase == HashJoinPhase::PROBE && _probe_input_chunk == nullptr;
}

bool HashJoiner::has_output() const {
    if (_phase == HashJoinPhase::BUILD) {
        return false;
    }

    if (_phase == HashJoinPhase::PROBE) {
        return _probe_input_chunk != nullptr;
    }

    if (_phase == HashJoinPhase::POST_PROBE) {
        // Only RIGHT ANTI-JOIN, RIGHT OUTER-JOIN, FULL OUTER-JOIN has HashJoinPhase::POST_PROBE,
        // in this phase, has_output() returns true until HashJoiner enters into HashJoinPhase::DONE.
        return true;
    }

    return false;
}

void HashJoiner::push_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    DCHECK(chunk && !chunk->is_empty());
    DCHECK(!_probe_input_chunk);

    _probe_input_chunk = std::move(chunk);
    _ht_has_remain = true;
    _prepare_probe_key_columns();
}

StatusOr<ChunkPtr> HashJoiner::pull_chunk(RuntimeState* state) {
    DCHECK(_phase != HashJoinPhase::BUILD);
    return _pull_probe_output_chunk(state);
}

StatusOr<ChunkPtr> HashJoiner::_pull_probe_output_chunk(RuntimeState* state) {
    DCHECK(_phase != HashJoinPhase::BUILD);

    auto chunk = std::make_shared<Chunk>();

    if (_phase == HashJoinPhase::PROBE || _probe_input_chunk != nullptr) {
        DCHECK(_ht_has_remain && _probe_input_chunk);

        TRY_CATCH_BAD_ALLOC(_ht.probe(state, _key_columns, &_probe_input_chunk, &chunk, &_ht_has_remain));
        if (!_ht_has_remain) {
            _probe_input_chunk = nullptr;
        }

        _filter_probe_output_chunk(chunk);

        return chunk;
    }

    if (_phase == HashJoinPhase::POST_PROBE) {
        if (!_need_post_probe()) {
            enter_eos_phase();
            return chunk;
        }

        TRY_CATCH_BAD_ALLOC(_ht.probe_remain(state, &chunk, &_ht_has_remain));
        if (!_ht_has_remain) {
            enter_eos_phase();
        }

        _filter_post_probe_output_chunk(chunk);

        return chunk;
    }

    return chunk;
}

void HashJoiner::close(RuntimeState* state) {
    _ht.close();
}

Status HashJoiner::create_runtime_filters(RuntimeState* state) {
    if (_phase != HashJoinPhase::BUILD) {
        return Status::OK();
    }

    uint64_t runtime_join_filter_pushdown_limit = 1024000;
    if (state->query_options().__isset.runtime_join_filter_pushdown_limit) {
        runtime_join_filter_pushdown_limit = state->query_options().runtime_join_filter_pushdown_limit;
    }

    if (_is_push_down) {
        if (_probe_node_type == TPlanNodeType::EXCHANGE_NODE && _build_node_type == TPlanNodeType::EXCHANGE_NODE) {
            _is_push_down = false;
        } else if (_ht.get_row_count() > runtime_join_filter_pushdown_limit) {
            _is_push_down = false;
        }

        if (_is_push_down || !_build_conjunct_ctxs_is_empty) {
            // In filter could be used to fast compute segment row range in storage engine
            RETURN_IF_ERROR(_create_runtime_in_filters(state));
        }
    }

    // it's quite critical to put publish runtime filters before short-circuit of
    // "inner-join with empty right table". because for global runtime filter
    // merge node is waiting for all partitioned runtime filter, so even hash row count is zero
    // we still have to build it.
    return _create_runtime_bloom_filters(state, runtime_join_filter_pushdown_limit);
}

void HashJoiner::reference_hash_table(HashJoiner* src_join_builder) {
    _ht = src_join_builder->_ht.clone_readable_table();
    _ht.set_probe_profile(_search_ht_timer, _output_probe_column_timer, _output_tuple_column_timer);

    _probe_column_count = src_join_builder->_probe_column_count;
    _build_column_count = src_join_builder->_build_column_count;
}

void HashJoiner::set_builder_finished() {
    set_finished();
    for (auto& prober : _read_only_join_probers) {
        prober->set_finished();
    }
}

void HashJoiner::set_prober_finished() {
    if (--_num_unfinished_probers == 0) {
        set_finished();
    }
}

bool HashJoiner::_has_null(const ColumnPtr& column) {
    if (column->is_nullable()) {
        const auto& null_column = ColumnHelper::as_raw_column<NullableColumn>(column)->null_column();
        DCHECK_GT(null_column->size(), 0);
        return null_column->contain_value(1, null_column->size(), 1);
    }
    return false;
}

Status HashJoiner::_build(RuntimeState* state) {
    {
        SCOPED_TIMER(_build_conjunct_evaluate_timer);
        // Currently, in order to implement simplicity, HashJoiner uses BigChunk,
        // Splice the Chunks from Scan on the right table into a big Chunk
        // In some scenarios, such as when the left and right tables are selected incorrectly
        // or when the large table is joined, the (BinaryColumn) in the Chunk exceeds the range of uint32_t,
        // which will cause the output of wrong data.
        // Currently, a defense needs to be added.
        // After a better solution is available, the BigChunk mechanism can be removed.
        if (_ht.get_build_chunk()->reach_capacity_limit()) {
            return Status::InternalError("Total size of single column exceed the limit of hash join");
        }
        TRY_CATCH_BAD_ALLOC(_prepare_build_key_columns());
    }

    {
        SCOPED_TIMER(_build_ht_timer);
        TRY_CATCH_BAD_ALLOC(_ht.build(state));
    }

    return Status::OK();
}

void HashJoiner::_calc_filter_for_other_conjunct(ChunkPtr* chunk, Column::Filter& filter, bool& filter_all,
                                                 bool& hit_all) {
    filter_all = false;
    hit_all = false;
    filter.assign((*chunk)->num_rows(), 1);

    for (auto* ctx : _other_join_conjunct_ctxs) {
        ColumnPtr column = ctx->evaluate((*chunk).get());
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
}

void HashJoiner::_process_row_for_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count,
                                                 bool filter_all, bool hit_all, const Column::Filter& filter) {
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

void HashJoiner::_process_outer_join_with_other_conjunct(ChunkPtr* chunk, size_t start_column, size_t column_count) {
    bool filter_all = false;
    bool hit_all = false;
    Column::Filter filter;

    _calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all);
    _process_row_for_other_conjunct(chunk, start_column, column_count, filter_all, hit_all, filter);

    _ht.remove_duplicate_index(&filter);
    (*chunk)->filter(filter);
}

void HashJoiner::_process_semi_join_with_other_conjunct(ChunkPtr* chunk) {
    bool filter_all = false;
    bool hit_all = false;
    Column::Filter filter;

    _calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all);

    _ht.remove_duplicate_index(&filter);
    (*chunk)->filter(filter);
}

void HashJoiner::_process_right_anti_join_with_other_conjunct(ChunkPtr* chunk) {
    bool filter_all = false;
    bool hit_all = false;
    Column::Filter filter;

    _calc_filter_for_other_conjunct(chunk, filter, filter_all, hit_all);

    _ht.remove_duplicate_index(&filter);
    (*chunk)->set_num_rows(0);
}

void HashJoiner::_process_other_conjunct(ChunkPtr* chunk) {
    SCOPED_TIMER(_other_join_conjunct_evaluate_timer);
    switch (_join_type) {
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::FULL_OUTER_JOIN:
        _process_outer_join_with_other_conjunct(chunk, _probe_column_count, _build_column_count);
        break;
    case TJoinOp::RIGHT_OUTER_JOIN:
    case TJoinOp::LEFT_SEMI_JOIN:
    case TJoinOp::LEFT_ANTI_JOIN:
    case TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN:
    case TJoinOp::RIGHT_SEMI_JOIN:
        _process_semi_join_with_other_conjunct(chunk);
        break;
    case TJoinOp::RIGHT_ANTI_JOIN:
        _process_right_anti_join_with_other_conjunct(chunk);
        break;
    default:
        // the other join conjunct for inner join will be convert to other predicate
        // so can't reach here
        ExecNode::eval_conjuncts(_other_join_conjunct_ctxs, (*chunk).get());
    }
}

void HashJoiner::_process_where_conjunct(ChunkPtr* chunk) {
    SCOPED_TIMER(_where_conjunct_evaluate_timer);
    ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get());
}

} // namespace starrocks::vectorized
