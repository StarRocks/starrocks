// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/aggregate/distinct_blocking_node.h"

namespace starrocks::vectorized {

Status DistinctBlockingNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_group_by_expr_ctxs, state));
    RETURN_IF_ERROR(_children[0]->open(state));

    ChunkPtr chunk;
    bool limit_with_no_agg = limit() != -1;
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
            if (false) {
            }
#define HASH_SET_METHOD(NAME)                                                                        \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME)                                   \
            _build_hash_set<decltype(_hash_set_variant.NAME)::element_type>(*_hash_set_variant.NAME, \
                                                                            chunk->num_rows());
            APPLY_FOR_VARIANT_ALL(HASH_SET_METHOD)
#undef HASH_SET_METHOD

            _num_input_rows += chunk->num_rows();
            if (limit_with_no_agg) {
                auto size = _hash_set_variant.size();
                if (size >= limit()) {
                    break;
                }
            }

            RETURN_IF_ERROR(_check_hash_set_memory_usage(state));
        }
    }

    COUNTER_SET(_hash_table_size, (int64_t)_hash_set_variant.size());

    // If hash set is empty, we don't need to return value
    if (_hash_set_variant.size() == 0) {
        _is_finished = true;
    }

    if (false) {
    }
#define HASH_SET_METHOD(NAME) \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME) _it_hash = _hash_set_variant.NAME->hash_set.begin();
    APPLY_FOR_VARIANT_ALL(HASH_SET_METHOD)
#undef HASH_SET_METHOD

    COUNTER_SET(_input_row_count, _num_input_rows);
    return Status::OK();
}

Status DistinctBlockingNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
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

    if (false) {
    }
#define HASH_SET_METHOD(NAME)                                                                                   \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME)                                              \
            _convert_hash_set_to_chunk<decltype(_hash_set_variant.NAME)::element_type>(*_hash_set_variant.NAME, \
                                                                                       chunk_size, chunk);
    APPLY_FOR_VARIANT_ALL(HASH_SET_METHOD)
#undef HASH_SET_METHOD

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