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

#include "aggregate_distinct_streaming_sink_operator.h"

#include <variant>

#include "runtime/current_thread.h"
#include "simd/simd.h"
namespace starrocks::pipeline {

Status AggregateDistinctStreamingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::LIMITED_MEM) {
        _limited_mem_state.limited_memory_size = config::streaming_agg_limited_memory_size;
    }
    return _aggregator->open(state);
}

void AggregateDistinctStreamingSinkOperator::close(RuntimeState* state) {
    auto* counter = ADD_COUNTER(_unique_metrics, "HashTableMemoryUsage", TUnit::BYTES);
    counter->set(_aggregator->hash_set_memory_usage());
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateDistinctStreamingSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    // skip processing if cancelled
    if (state->is_cancelled()) {
        return Status::OK();
    }

    if (_aggregator->hash_set_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<ChunkPtr> AggregateDistinctStreamingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

void AggregateDistinctStreamingSinkOperator::set_execute_mode(int performance_level) {
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::AUTO) {
        _aggregator->streaming_preaggregation_mode() = TStreamingPreaggregationMode::LIMITED_MEM;
    }
    _limited_mem_state.limited_memory_size = _aggregator->hash_map_memory_usage();
}

Status AggregateDistinctStreamingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();

    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));

    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
        return _push_chunk_by_force_streaming(chunk);
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
        return _push_chunk_by_force_preaggregation(chunk->num_rows());
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::LIMITED_MEM) {
        return _push_chunk_by_limited_memory(chunk, chunk_size);
    } else {
        return _push_chunk_by_auto(chunk, chunk->num_rows());
    }
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_force_streaming(const ChunkPtr& chunk) {
    SCOPED_TIMER(_aggregator->streaming_timer());
    ChunkPtr res = std::make_shared<Chunk>();
    RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
    _aggregator->offer_chunk_to_buffer(res);
    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_force_preaggregation(const size_t chunk_size) {
    SCOPED_TIMER(_aggregator->agg_compute_timer());

    _aggregator->build_hash_set(chunk_size);

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

    TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());

    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_limited_memory(const ChunkPtr& chunk,
                                                                             const size_t chunk_size) {
    bool ht_needs_expansion = _aggregator->hash_set_variant().need_expand(chunk_size);
    if (ht_needs_expansion && _limited_mem_state.has_limited(*_aggregator)) {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
        _aggregator->set_streaming_all_states(true);
    } else {
        RETURN_IF_ERROR(_push_chunk_by_auto(chunk, chunk_size));
    }
    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_auto(const ChunkPtr& chunk, const size_t chunk_size) {
    bool ht_needs_expansion = _aggregator->hash_set_variant().need_expand(chunk_size);
    size_t allocated_bytes = _aggregator->hash_set_variant().allocated_memory_usage(_aggregator->mem_pool());
    if (!ht_needs_expansion ||
        _aggregator->should_expand_preagg_hash_tables(_aggregator->num_input_rows(), chunk_size, allocated_bytes,
                                                      _aggregator->hash_set_variant().size())) {
        // hash table is not full or allow expand the hash table according reduction rate
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set(chunk_size));
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

        TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());
    } else {
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set_with_selection(chunk_size));
        }

        {
            SCOPED_TIMER(_aggregator->streaming_timer());
            size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection());
            if (zero_count == 0) {
                ChunkPtr res = std::make_shared<Chunk>();
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
                _aggregator->offer_chunk_to_buffer(res);
            } else if (zero_count != _aggregator->streaming_selection().size()) {
                ChunkPtr res = std::make_shared<Chunk>();
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming_with_selection(chunk.get(), &res));
                _aggregator->offer_chunk_to_buffer(res);
            }
        }

        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    }

    return Status::OK();
}
Status AggregateDistinctStreamingSinkOperator::reset_state(RuntimeState* state,
                                                           const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    return _aggregator->reset_state(state, refill_chunks, this);
}
} // namespace starrocks::pipeline
