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

#include "aggregate_streaming_source_operator.h"

#include <variant>

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

StatusOr<ChunkPtr> AggregateStreamingSourceOperator::pull_chunk(RuntimeState* state) {
    // It is no need to distinguish whether streaming or aggregation mode
    // We just first read chunk from buffer and finally read chunk from hash table
    if (!_aggregator->is_chunk_buffer_empty()) {
        return _aggregator->poll_chunk_buffer();
    }

    // Even if it is streaming mode, the purpose of reading from hash table is to
    // correctly process the state of hash table(_is_ht_eos)
    ChunkPtr chunk = std::make_shared<Chunk>();
    RETURN_IF_ERROR(_output_chunk_from_hash_map(&chunk, state));
    eval_runtime_bloom_filters(chunk.get());
    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

Status AggregateStreamingSourceOperator::_output_chunk_from_hash_map(ChunkPtr* chunk, RuntimeState* state) {
    if (!_aggregator->it_hash().has_value()) {
        _aggregator->hash_map_variant().visit(
                [&](auto& hash_map_with_key) { _aggregator->it_hash() = _aggregator->_state_allocator.begin(); });
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    }

    RETURN_IF_ERROR(_aggregator->convert_hash_map_to_chunk(state->chunk_size(), chunk));
    return Status::OK();
}

} // namespace starrocks::pipeline
