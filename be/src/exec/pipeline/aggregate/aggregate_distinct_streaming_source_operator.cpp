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

    if (_aggregator->is_streaming_all_states()) {
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

StatusOr<ChunkPtr> AggregateDistinctStreamingSourceOperator::pull_chunk(RuntimeState* state) {
    if (!_aggregator->is_chunk_buffer_empty()) {
        return _aggregator->poll_chunk_buffer();
    }

    ChunkPtr chunk = std::make_shared<Chunk>();
    RETURN_IF_ERROR(_output_chunk_from_hash_set(&chunk, state));
    eval_runtime_bloom_filters(chunk.get());
    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

Status AggregateDistinctStreamingSourceOperator::_output_chunk_from_hash_set(ChunkPtr* chunk, RuntimeState* state) {
    if (!_aggregator->it_hash().has_value()) {
        _aggregator->hash_set_variant().visit(
                [&](auto& hash_set_with_key) { _aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    }

    _aggregator->convert_hash_set_to_chunk(state->chunk_size(), chunk);

    if (_aggregator->is_streaming_all_states() && _aggregator->is_ht_eos()) {
        RETURN_IF_ERROR(_aggregator->reset_state(state, {}, nullptr));
        _aggregator->set_streaming_all_states(false);
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
