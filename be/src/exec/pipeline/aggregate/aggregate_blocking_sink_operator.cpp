// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "aggregate_blocking_sink_operator.h"

#include "runtime/current_thread.h"

namespace starrocks::pipeline {

Status AggregateBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    return _aggregator->open(state);
}

void AggregateBlockingSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateBlockingSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (!_aggregator->is_none_group_by_exprs()) {
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
        // If hash map is empty, we don't need to return value
        if (_aggregator->hash_map_variant().size() == 0) {
            _aggregator->set_ht_eos();
        }

        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                   \
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME) \
            _aggregator->it_hash() = _aggregator->_state_allocator.begin();
        APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    } else if (_aggregator->is_none_group_by_exprs()) {
        // for aggregate no group by, if _num_input_rows is 0,
        // In update phase, we directly return empty chunk.
        // In merge phase, we will handle it.
        if (_aggregator->num_input_rows() == 0 && !_aggregator->needs_finalize()) {
            _aggregator->set_ht_eos();
        }
    }
    COUNTER_UPDATE(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _aggregator->sink_complete();
    return Status::OK();
}

Status AggregateBlockingSinkOperator::reset_state(std::vector<ChunkPtr>&& chunks) {
    _is_finished = false;
    return _aggregator->reset_state(std::move(chunks));
}

StatusOr<vectorized::ChunkPtr> AggregateBlockingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AggregateBlockingSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    RETURN_IF_ERROR(_aggregator->evaluate_exprs(chunk.get()));

    bool agg_group_by_with_limit =
            (!_aggregator->is_none_group_by_exprs() &&     // has group by
             _aggregator->limit() != -1 &&                 // has limit
             _aggregator->conjunct_ctxs().empty() &&       // no 'having' clause
             _aggregator->get_aggr_phase() == AggrPhase2); // phase 2, keep it to make things safe
    const auto chunk_size = chunk->num_rows();
    DCHECK_LE(chunk_size, state->chunk_size());

    SCOPED_TIMER(_aggregator->agg_compute_timer());
    if (!_aggregator->is_none_group_by_exprs()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                          \
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME) {                      \
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                *_aggregator->hash_map_variant().NAME, chunk_size, agg_group_by_with_limit));                          \
    }
        APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD

        _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
        TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());
    }
    if (_aggregator->is_none_group_by_exprs()) {
        _aggregator->compute_single_agg_state(chunk_size);
    } else {
        if (agg_group_by_with_limit) {
            // use `_aggregator->streaming_selection()` here to mark whether needs to filter key when compute agg states,
            // it's generated in `build_hash_map`
            size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection().data(), chunk_size);
            if (zero_count == chunk_size) {
                _aggregator->compute_batch_agg_states(chunk_size);
            } else {
                _aggregator->compute_batch_agg_states_with_selection(chunk_size);
            }
        } else {
            _aggregator->compute_batch_agg_states(chunk_size);
        }
    }
    _aggregator->update_num_input_rows(chunk_size);
    RETURN_IF_ERROR(_aggregator->check_has_error());

    return Status::OK();
}
} // namespace starrocks::pipeline
