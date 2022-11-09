// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/aggregate/sorted_aggregate_streaming_source_operator.h"

#include "exec/pipeline/sort/sort_context.h"
#include "exec/vectorized/sorted_streaming_aggregator.h"

namespace starrocks::pipeline {
SortedAggregateStreamingSourceOperator::SortedAggregateStreamingSourceOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
        std::shared_ptr<SortedStreamingAggregator> aggregator)
        : SourceOperator(factory, id, "sorted_aggregate_streaming_source", plan_node_id, driver_sequence),
          _aggregator(std::move(aggregator)) {
    _aggregator->ref();
}

bool SortedAggregateStreamingSourceOperator::has_output() const {
    if (!_aggregator->is_chunk_buffer_empty()) {
        // There are two cases where chunk buffer is not empty
        // case1: streaming mode is 'FORCE_STREAMING'
        // case2: streaming mode is 'AUTO'
        //     case 2.1: very poor aggregation
        //     case 2.2: middle cases, first aggregate locally and output by stream
        return true;
    }
    return _aggregator->is_sink_complete() && !_aggregator->is_ht_eos();
}

bool SortedAggregateStreamingSourceOperator::is_finished() const {
    // source operator may finish early
    if (_is_finished) {
        return true;
    }

    if (_aggregator->is_sink_complete() && _aggregator->is_chunk_buffer_empty() && _aggregator->is_ht_eos()) {
        _is_finished = true;
    }
    return _is_finished;
}

Status SortedAggregateStreamingSourceOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status SortedAggregateStreamingSourceOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

void SortedAggregateStreamingSourceOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> SortedAggregateStreamingSourceOperator::pull_chunk(RuntimeState* state) {
    DCHECK(has_output());
    vectorized::ChunkPtr chunk;
    if (!_aggregator->is_chunk_buffer_empty()) {
        chunk = _aggregator->poll_chunk_buffer();
    } else {
        ASSIGN_OR_RETURN(chunk, _aggregator->pull_eos_chunk());
        _aggregator->set_ht_eos();
    }
    if (chunk == nullptr) {
        return chunk;
    }
    size_t old_size = chunk->num_rows();
    eval_runtime_bloom_filters(chunk.get());

    // For having
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_aggregator->conjunct_ctxs(), chunk.get()));
    _aggregator->update_num_rows_returned(-(old_size - chunk->num_rows()));
    DCHECK_CHUNK(chunk);

    return chunk;
}

OperatorPtr SortedAggregateStreamingSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                  int32_t driver_sequence) {
    return std::make_shared<SortedAggregateStreamingSourceOperator>(
            this, _id, _plan_node_id, driver_sequence, _aggregator_factory->get_or_create(driver_sequence));
}

} // namespace starrocks::pipeline
