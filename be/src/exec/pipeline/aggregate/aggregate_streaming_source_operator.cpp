// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "aggregate_streaming_source_operator.h"

namespace starrocks::pipeline {

bool AggregateStreamingSourceOperator::has_output() const {
    if (!_aggregator->is_chunk_buffer_empty()) {
        // There are two cases where chunk buffer is not empty
        // case1: streaming mode is 'FORCE_STREAMING'
        // case2: streaming mode is 'AUTO'
        //     case 2.1: very poor aggregation
        //     case 2.2: middle cases, first aggregate locally and output by stream
        return true;
    }

    // There are four cases where chunk buffer is empty
    // case1: streaming mode is 'FORCE_STREAMING'
    // case2: streaming mode is 'AUTO'
    //     case 2.1: very poor aggregation
    //     case 2.2: middle cases, first aggregate locally and output by stream
    // case3: streaming mode is 'FORCE_PREAGGREGATION'
    // case4: streaming mode is 'AUTO'
    //     case 4.1: very high aggregation
    //
    // case1 and case2 means that it will wait for the next chunk from the buffer
    // case3 and case4 means that it will apply local aggregate, so need to wait sink operator finish
    return _aggregator->is_sink_complete() && !_aggregator->is_ht_eos();
}

bool AggregateStreamingSourceOperator::is_finished() const {
    return _aggregator->is_sink_complete() && _aggregator->is_chunk_buffer_empty() && _aggregator->is_ht_eos();
}

Status AggregateStreamingSourceOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

void AggregateStreamingSourceOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> AggregateStreamingSourceOperator::pull_chunk(RuntimeState* state) {
    // It is no need to distinguish whether streaming or aggregation mode
    // We just first read chunk from buffer and finally read chunk from hash table
    if (!_aggregator->is_chunk_buffer_empty()) {
        return _aggregator->poll_chunk_buffer();
    }

    // Even if it is streaming mode, the purpose of reading from hash table is to
    // correctly process the state of hash table(_is_ht_eos)
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _output_chunk_from_hash_map(&chunk, state);
    eval_runtime_bloom_filters(chunk.get());
    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

void AggregateStreamingSourceOperator::_output_chunk_from_hash_map(vectorized::ChunkPtr* chunk, RuntimeState* state) {
    if (!_aggregator->it_hash().has_value()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                   \
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME) \
            _aggregator->it_hash() = _aggregator->_state_allocator.begin();
        APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    }

    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                                     \
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME)                   \
            _aggregator->convert_hash_map_to_chunk<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, state->chunk_size(), chunk);
    APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }
}

} // namespace starrocks::pipeline
