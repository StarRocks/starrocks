// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "aggregate_distinct_blocking_source_operator.h"

#include "exec/exec_node.h"

namespace starrocks::pipeline {

bool AggregateDistinctBlockingSourceOperator::has_output() const {
    return _aggregator->is_sink_complete() && !_aggregator->is_ht_eos();
}

bool AggregateDistinctBlockingSourceOperator::is_finished() const {
    return _aggregator->is_sink_complete() && _aggregator->is_ht_eos();
}

Status AggregateDistinctBlockingSourceOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

void AggregateDistinctBlockingSourceOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> AggregateDistinctBlockingSourceOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);

    int32_t chunk_size = state->chunk_size();
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();

    if (false) {
    }
#define HASH_SET_METHOD(NAME)                                                                                     \
    else if (_aggregator->hash_set_variant().type == vectorized::HashSetVariant::Type::NAME)                      \
            _aggregator->convert_hash_set_to_chunk<decltype(_aggregator->hash_set_variant().NAME)::element_type>( \
                    *_aggregator->hash_set_variant().NAME, chunk_size, &chunk);
    APPLY_FOR_VARIANT_ALL(HASH_SET_METHOD)
#undef HASH_SET_METHOD

    size_t old_size = chunk->num_rows();
    eval_runtime_bloom_filters(chunk.get());

    // For having
    eval_conjuncts_and_in_filters(_aggregator->conjunct_ctxs(), chunk.get());
    _aggregator->update_num_rows_returned(-(old_size - chunk->num_rows()));

    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}
} // namespace starrocks::pipeline
