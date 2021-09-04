// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/aggregate/aggregate_blocking_node.h"

namespace starrocks::vectorized {

Status AggregateBlockingNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_group_by_expr_ctxs, state));
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_agg_expr_ctxs[i], state));
    }

    // Initial for FunctionContext of every aggregate functions
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        // initial const columns for i'th FunctionContext.
        _evaluate_const_columns(i);
    }

    RETURN_IF_ERROR(_children[0]->open(state));

    ChunkPtr chunk;

    VLOG_ROW << "_group_by_expr_ctxs size " << _group_by_expr_ctxs.size() << " _needs_finalize " << _needs_finalize;
    while (true) {
        bool eos = false;
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_children[0]->get_next(state, &chunk, &eos));

        if (eos) {
            break;
        }

        if (chunk->is_empty()) {
            continue;
        }

        DCHECK_LE(chunk->num_rows(), config::vector_chunk_size);

        _evaluate_exprs(chunk.get());

        {
            SCOPED_TIMER(_agg_compute_timer);
            if (!_group_by_expr_ctxs.empty()) {
                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                                        \
    else if (_hash_map_variant.type == HashMapVariant::Type::NAME)                                   \
            _build_hash_map<decltype(_hash_map_variant.NAME)::element_type>(*_hash_map_variant.NAME, \
                                                                            chunk->num_rows());
                APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD

                RETURN_IF_ERROR(_check_hash_map_memory_usage(state));
                _try_convert_to_two_level_map();
            }
            (this->*_compute_agg_states)(chunk->num_rows());

            _num_input_rows += chunk->num_rows();
        }
    }

    if (!_group_by_expr_ctxs.empty()) {
        COUNTER_SET(_hash_table_size, (int64_t)_hash_map_variant.size());
        // If hash map is empty, we don't need to return value
        if (_hash_map_variant.size() == 0) {
            _is_finished = true;
        }

        if (false) {
        }
#define HASH_MAP_METHOD(NAME) \
    else if (_hash_map_variant.type == HashMapVariant::Type::NAME) _it_hash = _hash_map_variant.NAME->hash_map.begin();
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    } else if (_group_by_expr_ctxs.empty()) {
        // for aggregate no group by, if _num_input_rows is 0,
        // In update phase, we directly return empty chunk.
        // In merge phase, we will handle it.
        if (_num_input_rows == 0 && !_needs_finalize) {
            _is_finished = true;
        }
    }
    COUNTER_SET(_input_row_count, _num_input_rows);
    return Status::OK();
}

Status AggregateBlockingNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    *eos = false;

    if (_is_finished) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        *eos = true;
        return Status::OK();
    }
    int32_t chunk_size = config::vector_chunk_size;

    if (_group_by_expr_ctxs.empty()) {
        SCOPED_TIMER(_get_results_timer);
        _convert_to_chunk_no_groupby(chunk);
    } else {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                   \
    else if (_hash_map_variant.type == HashMapVariant::Type::NAME)                                              \
            _convert_hash_map_to_chunk<decltype(_hash_map_variant.NAME)::element_type>(*_hash_map_variant.NAME, \
                                                                                       chunk_size, chunk);
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    }

    eval_join_runtime_filters(chunk->get());

    // For having
    size_t old_size = (*chunk)->num_rows();
    ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get());
    _num_rows_returned -= (old_size - (*chunk)->num_rows());

    _process_limit(chunk);

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

} // namespace starrocks::vectorized