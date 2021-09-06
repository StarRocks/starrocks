// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_blocking_operator.h"

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "config.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

void AggregateBlockingOperator::finish(RuntimeState* state) {
    if (_is_pre_finished) return;
    _is_pre_finished = true;

    if (!_group_by_expr_ctxs.empty()) {
        COUNTER_SET(_hash_table_size, (int64_t)_hash_map_variant.size());
        // If hash map is empty, we don't need to return value
        if (_hash_map_variant.size() == 0) {
            _is_finished = true;
        }

        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                             \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME) _it_hash = \
            _hash_map_variant.NAME->hash_map.begin();
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
}

StatusOr<vectorized::ChunkPtr> AggregateBlockingOperator::pull_chunk(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    // TODO(hcf) force annotation
    // RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    if (_is_finished) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        return Status::OK();
    }

    int32_t chunk_size = config::vector_chunk_size;
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();

    if (_group_by_expr_ctxs.empty()) {
        SCOPED_TIMER(_get_results_timer);
        _convert_to_chunk_no_groupby(&chunk);
    } else {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                   \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME)                                  \
            _convert_hash_map_to_chunk<decltype(_hash_map_variant.NAME)::element_type>(*_hash_map_variant.NAME, \
                                                                                       chunk_size, &chunk);
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    }

    // TODO(hcf) force annotation
    // eval_join_runtime_filters(chunk.get());

    // For having
    size_t old_size = chunk->num_rows();

    // TODO(hcf) force annotation
    // ExecNode::eval_conjuncts(_conjunct_ctxs, chunk.get());
    _num_rows_returned -= (old_size - chunk->num_rows());

    _process_limit(&chunk);

    DCHECK_CHUNK(chunk);

    return chunk;
}

Status AggregateBlockingOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (chunk->is_empty()) {
        return Status::OK();
    }

    DCHECK_LE(chunk->num_rows(), config::vector_chunk_size);
    _evaluate_group_by_exprs(chunk.get());
    _evaluate_agg_fn_exprs(chunk.get());

    {
        SCOPED_TIMER(_agg_compute_timer);
        if (!_group_by_expr_ctxs.empty()) {
            if (false) {
            }
#define HASH_MAP_METHOD(NAME)                                                                        \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME)                       \
            _build_hash_map<decltype(_hash_map_variant.NAME)::element_type>(*_hash_map_variant.NAME, \
                                                                            chunk->num_rows());
            APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD

            RETURN_IF_ERROR(_check_hash_map_memory_usage(state));
            _try_convert_to_two_level_map();
        }
        if (_group_by_expr_ctxs.empty()) {
            _compute_single_agg_state(chunk->num_rows());
        } else {
            _compute_batch_agg_states(chunk->num_rows());
        }
        _num_input_rows += chunk->num_rows();
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
