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

#include "exec/stream/aggregate/stream_aggregate_operator.h"

#include "exec/exec_node.h"

namespace starrocks::stream {

bool StreamAggregateOperator::has_output() const {
    return _is_epoch_finished && _has_output;
}

bool StreamAggregateOperator::is_finished() const {
    return _is_input_finished && !_has_output;
}

Status StreamAggregateOperator::set_finishing(RuntimeState* state) {
    _is_input_finished = true;
    _aggregator->sink_complete();
    return Status::OK();
}

Status StreamAggregateOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

bool StreamAggregateOperator::is_epoch_finished() const {
    return _is_epoch_finished && !_has_output;
}

Status StreamAggregateOperator::set_epoch_finishing(RuntimeState* state) {
    _is_epoch_finished = true;
    return Status::OK();
}

Status StreamAggregateOperator::set_epoch_finished(RuntimeState* state) {
    // TODO:  async flush state
    // ATTENTION:
    // 1. reset state to reduce memory usage.
    // 2. reset state will change `_aggregator->is_ht_eos()`
    RETURN_IF_ERROR(_aggregator->reset_state(state));
    return Status::OK();
}

Status StreamAggregateOperator::reset_epoch(RuntimeState* state) {
    _is_epoch_finished = false;
    _has_output = true;
    return Status::OK();
}

Status StreamAggregateOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    return _aggregator->open(state);
}

void StreamAggregateOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

Status StreamAggregateOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_aggregator->process_chunk(dynamic_cast<StreamChunk*>(chunk.get())));
    return Status::OK();
}

StatusOr<ChunkPtr> StreamAggregateOperator::pull_chunk(RuntimeState* state) {
    DCHECK(!_aggregator->is_none_group_by_exprs());
    RETURN_IF_CANCELLED(state);

    StreamChunkPtr chunk = std::make_shared<StreamChunk>();
    const auto chunk_size = state->chunk_size();
    RETURN_IF_ERROR(_aggregator->output_changes(chunk_size, &chunk));

    // For having
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_aggregator->conjunct_ctxs(), chunk.get()));
    _has_output = !_aggregator->is_ht_eos();
    return std::move(chunk);
}

} // namespace starrocks::stream
