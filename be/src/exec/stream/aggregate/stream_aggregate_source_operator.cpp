// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/stream/aggregate/stream_aggregate_source_operator.h"

#include "exec/exec_node.h"

namespace starrocks::stream {

bool StreamAggregateSourceOperator::is_finished() const {
    return _stream_aggregator->is_sink_complete() && _stream_aggregator->is_ht_eos();
}

Status StreamAggregateSourceOperator::set_finished(RuntimeState* state) {
    return _stream_aggregator->set_finished();
}

bool StreamAggregateSourceOperator::has_output() const {
    return _stream_aggregator->is_epoch_finished() && !_stream_aggregator->is_ht_eos();
}

bool StreamAggregateSourceOperator::is_epoch_finished() const {
    return _stream_aggregator->is_epoch_finished() && _stream_aggregator->is_ht_eos();
}

Status StreamAggregateSourceOperator::set_epoch_finishing(RuntimeState* state) {
    return Status::OK();
}

Status StreamAggregateSourceOperator::set_epoch_finished(RuntimeState* state) {
    // TODO: assync refresh.
    // TODO: only commit when the last operator finished.
    RETURN_IF_ERROR(_stream_aggregator->commit_epoch(state));
    return _stream_aggregator->reset_state(state);
}

Status StreamAggregateSourceOperator::reset_epoch(RuntimeState* state) {
    return Status::OK();
}

void StreamAggregateSourceOperator::close(RuntimeState* state) {
    _stream_aggregator->unref(state);
    Operator::close(state);
}

StatusOr<ChunkPtr> StreamAggregateSourceOperator::pull_chunk(RuntimeState* state) {
    DCHECK(!_stream_aggregator->is_none_group_by_exprs());
    RETURN_IF_CANCELLED(state);

    StreamChunkPtr stream_chunk = std::make_shared<StreamChunk>();
    const auto chunk_size = state->chunk_size();
    RETURN_IF_ERROR(_stream_aggregator->output_changes(chunk_size, &stream_chunk));

    {
        // TODO(lism): support filter ops cols
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_stream_aggregator->conjunct_ctxs(), stream_chunk.get()));
    }

    return std::move(stream_chunk);
}

} // namespace starrocks::stream
