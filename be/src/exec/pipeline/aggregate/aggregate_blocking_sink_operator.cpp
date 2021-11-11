// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_blocking_sink_operator.h"

namespace starrocks::pipeline {

Status AggregateBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    // _aggregator is shared by sink operator and source operator
    // we must only prepare it at sink operator
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), get_runtime_profile(), _mem_tracker.get()));
    return _aggregator->open(state);
}

bool AggregateBlockingSinkOperator::is_finished() const {
    return _is_finished;
}

void AggregateBlockingSinkOperator::finish(RuntimeState* state) {
    if (_is_finished) {
        return;
    }
    _is_finished = true;

    if (!_aggregator->is_none_group_by_exprs()) {
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
        // If hash map is empty, we don't need to return value
        if (_aggregator->hash_map_variant().size() == 0) {
            _aggregator->set_ht_eos();
        }

        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                         \
    else if (_aggregator->hash_map_variant().type == vectorized::HashMapVariant::Type::NAME) _aggregator->it_hash() = \
            _aggregator->hash_map_variant().NAME->hash_map.begin();
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    } else if (_aggregator->is_none_group_by_exprs()) {
        // for aggregate no group by, if _num_input_rows is 0,
        // In update phase, we directly return empty chunk.
        // In merge phase, we will handle it.
        if (_aggregator->num_input_rows() == 0 && !_aggregator->needs_finalize()) {
            _aggregator->set_ht_eos();
        }
    }
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _aggregator->sink_complete();
}

StatusOr<vectorized::ChunkPtr> AggregateBlockingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AggregateBlockingSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    DCHECK_LE(chunk->num_rows(), config::vector_chunk_size);
    _aggregator->evaluate_exprs(chunk.get());

    SCOPED_TIMER(_aggregator->agg_compute_timer());
    if (!_aggregator->is_none_group_by_exprs()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                          \
    else if (_aggregator->hash_map_variant().type == vectorized::HashMapVariant::Type::NAME)           \
            _aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, chunk->num_rows());
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD

        _mem_tracker->set(_aggregator->hash_map_variant().memory_usage() +
                          _aggregator->mem_pool()->total_reserved_bytes());
        _aggregator->try_convert_to_two_level_map();
    }
    if (_aggregator->is_none_group_by_exprs()) {
        _aggregator->compute_single_agg_state(chunk->num_rows());
    } else {
        _aggregator->compute_batch_agg_states(chunk->num_rows());
    }
    _aggregator->update_num_input_rows(chunk->num_rows());

    return Status::OK();
}
} // namespace starrocks::pipeline
