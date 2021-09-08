// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_streaming_operator.h"

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "common/status.h"
#include "config.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"

namespace starrocks::pipeline {

bool AggregateStreamingOperator::has_output() const {
    // There are two cases where _curr_chunk is not null
    // case1：streaming mode is 'FORCE_STREAMING'
    // case2：streaming mode is 'AUTO'
    //     case 2.1: very poor aggregation
    //     case 2.2: middle cases, first aggregate locally and output by stream
    if (_curr_chunk != nullptr) {
        return true;
    }

    // There are two cases where _curr_chunk is null,
    // it will apply local aggregate, so need to wait all the input chunks
    // case1：streaming mode is 'FORCE_PREAGGREGATION'
    // case2：streaming mode is 'AUTO'
    //     case 2.1: very high aggregation
    if (!_is_finished) {
        return false;
    }

    if (_is_ht_done || _hash_map_variant.size() == 0) {
        return false;
    }

    return true;
}

bool AggregateStreamingOperator::is_finished() const {
    if (!_is_finished) {
        return false;
    }

    return !has_output();
}

void AggregateStreamingOperator::finish(RuntimeState* state) {
    if (_is_finished) {
        return;
    }
    _is_finished = true;
}

StatusOr<vectorized::ChunkPtr> AggregateStreamingOperator::pull_chunk(RuntimeState* state) {
    if (_curr_chunk != nullptr) {
        vectorized::ChunkPtr chunk = std::move(_curr_chunk);
        _curr_chunk = nullptr;
        return chunk;
    }

    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _output_chunk_from_hash_map(&chunk);
    _process_limit(&chunk);
    DCHECK_CHUNK(chunk);
    return chunk;
}

Status AggregateStreamingOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();

    _num_input_rows += chunk_size;
    COUNTER_SET(_input_row_count, _num_input_rows);
    RETURN_IF_ERROR(_check_hash_map_memory_usage(state));

    _evaluate_group_by_exprs(chunk.get());
    _evaluate_agg_fn_exprs(chunk.get());

    if (_streaming_preaggregation_mode == TStreamingPreaggregationMode::FORCE_STREAMING) {
        return _push_chunk_by_force_streaming();
    } else if (_streaming_preaggregation_mode == TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
        return _push_chunk_by_force_preaggregation(chunk->num_rows());
    } else {
        return _push_chunk_by_auto(chunk->num_rows());
    }
}

Status AggregateStreamingOperator::_push_chunk_by_force_streaming() {
    // force execute streaming
    SCOPED_TIMER(_streaming_timer);
    _curr_chunk = std::make_shared<vectorized::Chunk>();
    _output_chunk_by_streaming(&_curr_chunk);
    return Status::OK();
}

Status AggregateStreamingOperator::_push_chunk_by_force_preaggregation(const size_t chunk_size) {
    SCOPED_TIMER(_agg_compute_timer);
    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                  \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME) \
            _build_hash_map<decltype(_hash_map_variant.NAME)::element_type>(*_hash_map_variant.NAME, chunk_size);
    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }

    if (_group_by_expr_ctxs.empty()) {
        _compute_single_agg_state(chunk_size);
    } else {
        _compute_batch_agg_states(chunk_size);
    }

    _try_convert_to_two_level_map();
    COUNTER_SET(_hash_table_size, (int64_t)_hash_map_variant.size());
    return Status::OK();
}

Status AggregateStreamingOperator::_push_chunk_by_auto(const size_t chunk_size) {
    size_t real_capacity = _hash_map_variant.capacity() - _hash_map_variant.capacity() / 8;
    size_t remain_size = real_capacity - _hash_map_variant.size();
    bool ht_needs_expansion = remain_size < chunk_size;
    if (!ht_needs_expansion ||
        _should_expand_preagg_hash_tables(chunk_size, _mem_pool->total_allocated_bytes(), _hash_map_variant.size())) {
        // hash table is not full or allow expand the hash table according reduction rate
        SCOPED_TIMER(_agg_compute_timer);
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                  \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME) \
            _build_hash_map<decltype(_hash_map_variant.NAME)::element_type>(*_hash_map_variant.NAME, chunk_size);
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }

        if (_group_by_expr_ctxs.empty()) {
            _compute_single_agg_state(chunk_size);
        } else {
            _compute_batch_agg_states(chunk_size);
        }

        _try_convert_to_two_level_map();
        COUNTER_SET(_hash_table_size, (int64_t)_hash_map_variant.size());
    } else {
        {
            SCOPED_TIMER(_agg_compute_timer);
            if (false) {
            }
#define HASH_MAP_METHOD(NAME)                                                         \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME)        \
            _build_hash_map<typename decltype(_hash_map_variant.NAME)::element_type>( \
                    *_hash_map_variant.NAME, chunk_size, &_streaming_selection);
            APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
            else {
                DCHECK(false);
            }
        }

        size_t zero_count = SIMD::count_zero(_streaming_selection);
        // very poor aggregation
        if (zero_count == 0) {
            SCOPED_TIMER(_streaming_timer);
            _curr_chunk = std::make_shared<vectorized::Chunk>();
            _output_chunk_by_streaming(&_curr_chunk);
        }
        // very high aggregation
        else if (zero_count == _streaming_selection.size()) {
            SCOPED_TIMER(_agg_compute_timer);
            _compute_batch_agg_states(chunk_size);
        } else {
            // middle cases, first aggregate locally and output by stream
            {
                SCOPED_TIMER(_agg_compute_timer);
                _compute_batch_agg_states(chunk_size, _streaming_selection);
            }
            {
                SCOPED_TIMER(_streaming_timer);
                _curr_chunk = std::make_shared<vectorized::Chunk>();
                _output_chunk_by_streaming(&_curr_chunk, _streaming_selection);
            }
        }

        COUNTER_SET(_hash_table_size, (int64_t)_hash_map_variant.size());
    }

    return Status::OK();
}

void AggregateStreamingOperator::_output_chunk_from_hash_map(vectorized::ChunkPtr* chunk) {
    if (!_it_hash.has_value()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                             \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME) _it_hash = \
            _hash_map_variant.NAME->hash_map.begin();
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }
        COUNTER_SET(_hash_table_size, (int64_t)_hash_map_variant.size());
    }

    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                           \
    else if (_hash_map_variant.type == vectorized::HashMapVariant::Type::NAME)          \
            _convert_hash_map_to_chunk<decltype(_hash_map_variant.NAME)::element_type>( \
                    *_hash_map_variant.NAME, config::vector_chunk_size, chunk);
    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }
}

} // namespace starrocks::pipeline