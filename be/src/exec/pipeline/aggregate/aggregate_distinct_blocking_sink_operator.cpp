// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "aggregate_distinct_blocking_sink_operator.h"

#include "runtime/current_thread.h"

namespace starrocks::pipeline {

Status AggregateDistinctBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    return _aggregator->open(state);
}

void AggregateDistinctBlockingSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateDistinctBlockingSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

    // If hash set is empty, we don't need to return value
    if (_aggregator->hash_set_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    if (false) {
    }
#define HASH_SET_METHOD(NAME)                                                                   \
    else if (_aggregator->hash_set_variant().type == vectorized::AggHashSetVariant::Type::NAME) \
            _aggregator->it_hash() = _aggregator->hash_set_variant().NAME->hash_set.begin();
    APPLY_FOR_AGG_VARIANT_ALL(HASH_SET_METHOD)
#undef HASH_SET_METHOD

    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> AggregateDistinctBlockingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AggregateDistinctBlockingSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    DCHECK_LE(chunk->num_rows(), state->chunk_size());
    RETURN_IF_ERROR(_aggregator->evaluate_exprs(chunk.get()));

    {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        bool limit_with_no_agg = _aggregator->limit() != -1;

        if (false) {
        }
#define HASH_SET_METHOD(NAME)                                                                                          \
    else if (_aggregator->hash_set_variant().type == vectorized::AggHashSetVariant::Type::NAME) {                      \
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set<decltype(_aggregator->hash_set_variant().NAME)::element_type>( \
                *_aggregator->hash_set_variant().NAME, chunk->num_rows()));                                            \
    }
        APPLY_FOR_AGG_VARIANT_ALL(HASH_SET_METHOD)
#undef HASH_SET_METHOD

        _mem_tracker->set(_aggregator->hash_set_variant().reserved_memory_usage(_aggregator->mem_pool()));
        TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());

        _aggregator->update_num_input_rows(chunk->num_rows());
        if (limit_with_no_agg) {
            auto size = _aggregator->hash_set_variant().size();
            if (size >= _aggregator->limit()) {
                // TODO(hcf) do something
            }
        }
    }

    return Status::OK();
}
Status AggregateDistinctBlockingSinkOperator::reset_state(RuntimeState* state,
                                                          const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    return _aggregator->reset_state(state, refill_chunks, this);
}
} // namespace starrocks::pipeline
