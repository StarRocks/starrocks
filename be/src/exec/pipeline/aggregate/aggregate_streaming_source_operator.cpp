// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "aggregate_streaming_source_operator.h"

namespace starrocks::pipeline {

bool AggregateStreamingSourceOperator::has_output() const {
    // There are two cases where _buffer_chunk is not null
    // case1：streaming mode is 'FORCE_STREAMING'
    // case2：streaming mode is 'AUTO'
    //     case 2.1: very poor aggregation
    //     case 2.2: middle cases, first aggregate locally and output by stream
    if (!_aggregator->is_chunk_buffer_empty()) {
        return true;
    }

    // There are two cases where _buffer_chunk is null,
    // it will apply local aggregate, so need to wait sink operator finish
    // case1：streaming mode is 'FORCE_PREAGGREGATION'
    // case2：streaming mode is 'AUTO'
    //     case 2.1: very high aggregation
    return _aggregator->is_sink_complete() && !_aggregator->is_ht_done();
}

bool AggregateStreamingSourceOperator::is_finished() const {
    // since there are two behavior of streaming operator
    // case 1: chunk-at-a-time, so we check whether the chunk buffer is empty
    // case 2: local aggregate, so we check whether hash table is done
    return _aggregator->is_sink_complete() && _aggregator->is_chunk_buffer_empty() && _aggregator->is_ht_done();
}

void AggregateStreamingSourceOperator::finish(RuntimeState* state) {
    _is_finished = true;
}

Status AggregateStreamingSourceOperator::close(RuntimeState* state) {
    // _aggregator is shared by sink operator and source operator
    // we must only close it at source operator
    RETURN_IF_ERROR(_aggregator->close(state));
    return SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> AggregateStreamingSourceOperator::pull_chunk(RuntimeState* state) {
    if (!_aggregator->is_chunk_buffer_empty()) {
        return std::move(_aggregator->poll_chunk_buffer());
    }

    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _output_chunk_from_hash_map(&chunk);
    _aggregator->process_limit(&chunk);
    DCHECK_CHUNK(chunk);
    return chunk;
}

void AggregateStreamingSourceOperator::_output_chunk_from_hash_map(vectorized::ChunkPtr* chunk) {
    if (!_aggregator->it_hash().has_value()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                         \
    else if (_aggregator->hash_map_variant().type == vectorized::HashMapVariant::Type::NAME) _aggregator->it_hash() = \
            _aggregator->hash_map_variant().NAME->hash_map.begin();
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    }

    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                                     \
    else if (_aggregator->hash_map_variant().type == vectorized::HashMapVariant::Type::NAME)                      \
            _aggregator->convert_hash_map_to_chunk<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, config::vector_chunk_size, chunk);
    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }
}
} // namespace starrocks::pipeline