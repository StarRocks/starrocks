// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "aggregate_distinct_streaming_source_operator.h"

namespace starrocks::pipeline {

bool AggregateDistinctStreamingSourceOperator::has_output() const {
    // There are two cases where chunk buffer is not null
    // case1：streaming mode is 'FORCE_STREAMING'
    // case2：streaming mode is 'AUTO'
    //     case 2.1: very poor aggregation
    //     case 2.2: middle cases, first aggregate locally and output by stream
    if (!_aggregator->is_chunk_buffer_empty()) {
        return true;
    }

    // There are two cases where chunk buffer is null,
    // it will apply local aggregate, so need to wait sink operator finish
    // case1：streaming mode is 'FORCE_PREAGGREGATION'
    // case2：streaming mode is 'AUTO'
    //     case 2.1: very high aggregation
    return _aggregator->is_sink_complete() && !_aggregator->is_ht_eos();
}

bool AggregateDistinctStreamingSourceOperator::is_finished() const {
    // since there are two behavior of streaming operator
    // case 1: chunk-at-a-time, so we check whether the chunk buffer is empty
    // case 2: local aggregate, so we check whether hash table is eos
    return _aggregator->is_sink_complete() && _aggregator->is_chunk_buffer_empty() && _aggregator->is_ht_eos();
}

Status AggregateDistinctStreamingSourceOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

void AggregateDistinctStreamingSourceOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> AggregateDistinctStreamingSourceOperator::pull_chunk(RuntimeState* state) {
    if (!_aggregator->is_chunk_buffer_empty()) {
        return _aggregator->poll_chunk_buffer();
    }

    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _output_chunk_from_hash_set(&chunk, state);
    eval_runtime_bloom_filters(chunk.get());
    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

void AggregateDistinctStreamingSourceOperator::_output_chunk_from_hash_set(vectorized::ChunkPtr* chunk,
                                                                           RuntimeState* state) {
    if (!_aggregator->it_hash().has_value()) {
        _aggregator->hash_set_variant().visit(
                [&](auto& hash_set_with_key) { _aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    }

    _aggregator->hash_set_variant().visit([&](auto& hash_set_with_key) {
        _aggregator->convert_hash_set_to_chunk(*hash_set_with_key, state->chunk_size(), chunk);
    });
}

} // namespace starrocks::pipeline
