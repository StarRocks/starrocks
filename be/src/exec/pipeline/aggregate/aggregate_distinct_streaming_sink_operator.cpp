// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_distinct_streaming_sink_operator.h"

#include "simd/simd.h"
namespace starrocks::pipeline {

Status AggregateDistinctStreamingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    // _aggregator is shared by sink operator and source operator
    // we must only prepare it at sink operator
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), get_runtime_profile(), _mem_tracker.get()));
    return _aggregator->open(state);
}

bool AggregateDistinctStreamingSinkOperator::is_finished() const {
    return _is_finished;
}

void AggregateDistinctStreamingSinkOperator::finish(RuntimeState* state) {
    _is_finished = true;
    _aggregator->sink_complete();
}

StatusOr<vectorized::ChunkPtr> AggregateDistinctStreamingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AggregateDistinctStreamingSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();

    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _aggregator->evaluate_exprs(chunk.get());

    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
        return _push_chunk_by_force_streaming();
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
        return _push_chunk_by_force_preaggregation(chunk->num_rows());
    } else {
        return _push_chunk_by_auto(chunk->num_rows());
    }
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_force_streaming() {
    SCOPED_TIMER(_aggregator->streaming_timer());
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _aggregator->output_chunk_by_streaming(&chunk);
    _aggregator->offer_chunk_to_buffer(chunk);
    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_force_preaggregation(const size_t chunk_size) {
    SCOPED_TIMER(_aggregator->agg_compute_timer());
    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                          \
    else if (_aggregator->hash_set_variant().type == vectorized::HashSetVariant::Type::NAME)           \
            _aggregator->build_hash_set<decltype(_aggregator->hash_set_variant().NAME)::element_type>( \
                    *_aggregator->hash_set_variant().NAME, chunk_size);
    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }

    if (_aggregator->group_by_expr_ctxs().empty()) {
        _aggregator->compute_single_agg_state(chunk_size);
    } else {
        _aggregator->compute_batch_agg_states(chunk_size);
    }

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_auto(const size_t chunk_size) {
    // TODO: calc the real capacity of hashtable, will add one interface in the class of habletable
    size_t real_capacity = _aggregator->hash_set_variant().capacity() - _aggregator->hash_set_variant().capacity() / 8;
    size_t remain_size = real_capacity - _aggregator->hash_set_variant().size();
    bool ht_needs_expansion = remain_size < chunk_size;
    if (!ht_needs_expansion ||
        // TODO(hcf) first param
        _aggregator->should_expand_preagg_hash_tables(0, chunk_size, _aggregator->mem_pool()->total_allocated_bytes(),
                                                      _aggregator->hash_set_variant().size())) {
        // hash table is not full or allow expand the hash table according reduction rate
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                          \
    else if (_aggregator->hash_set_variant().type == vectorized::HashSetVariant::Type::NAME)           \
            _aggregator->build_hash_set<decltype(_aggregator->hash_set_variant().NAME)::element_type>( \
                    *_aggregator->hash_set_variant().NAME, chunk_size);
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }

        if (_aggregator->group_by_expr_ctxs().empty()) {
            _aggregator->compute_single_agg_state(chunk_size);
        } else {
            _aggregator->compute_batch_agg_states(chunk_size);
        }

        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    } else {
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            if (false) {
            }
#define HASH_MAP_METHOD(NAME)                                                                                       \
    else if (_aggregator->hash_set_variant().type == vectorized::HashSetVariant::Type::NAME) _aggregator            \
            ->build_hash_set_with_selection<typename decltype(_aggregator->hash_set_variant().NAME)::element_type>( \
                    *_aggregator->hash_set_variant().NAME, chunk_size);
            APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
            else {
                DCHECK(false);
            }
        }

        {
            SCOPED_TIMER(_aggregator->streaming_timer());
            size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection());
            if (zero_count == 0) {
                vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
                _aggregator->output_chunk_by_streaming(&chunk);
                _aggregator->offer_chunk_to_buffer(chunk);
            } else if (zero_count != _aggregator->streaming_selection().size()) {
                vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
                _aggregator->output_chunk_by_streaming_with_selection(&chunk);
                _aggregator->offer_chunk_to_buffer(chunk);
            }
        }

        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    }

    return Status::OK();
}
} // namespace starrocks::pipeline