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

#include "exec/stream/join/stream_hash_join_probe_operator.h"

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "runtime/runtime_state.h"

namespace starrocks::stream {

// ============================================================================
// StreamHashJoinProbeOperator implementation
// ============================================================================

StreamHashJoinProbeOperator::StreamHashJoinProbeOperator(pipeline::OperatorFactory* factory, int32_t id,
                                                         int32_t plan_node_id, int32_t driver_sequence,
                                                         TJoinOp::type join_type,
                                                         std::vector<ExprContext*> probe_expr_ctxs,
                                                         std::vector<ExprContext*> build_expr_ctxs)
        : pipeline::Operator(factory, id, "stream_hash_join_probe", plan_node_id, false, driver_sequence),
          _join_type(join_type),
          _probe_expr_ctxs(std::move(probe_expr_ctxs)),
          _build_expr_ctxs(std::move(build_expr_ctxs)) {}

Status StreamHashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _runtime_state = state;

    // Create the underlying stream join operator
    _stream_join_operator = std::make_unique<StreamJoinOperator>(_join_type, _probe_expr_ctxs, _build_expr_ctxs);
    RETURN_IF_ERROR(_stream_join_operator->prepare(state));

    // Set up join state table if provided
    if (_join_state_table) {
        _stream_join_operator->set_join_state_table(std::move(_join_state_table));
    }

    return Status::OK();
}

void StreamHashJoinProbeOperator::close(RuntimeState* state) {
    if (_stream_join_operator) {
        _stream_join_operator->close(state);
    }
    Operator::close(state);
}

bool StreamHashJoinProbeOperator::has_output() const {
    return _output_chunk != nullptr || !_input_chunks.empty();
}

bool StreamHashJoinProbeOperator::need_input() const {
    return !_is_input_finished && _input_chunks.size() < 10; // Buffer up to 10 chunks
}

bool StreamHashJoinProbeOperator::is_finished() const {
    return _is_finished;
}

Status StreamHashJoinProbeOperator::set_finishing(RuntimeState* state) {
    _is_input_finished = true;
    return Status::OK();
}

Status StreamHashJoinProbeOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

bool StreamHashJoinProbeOperator::is_epoch_finished() const {
    return _is_epoch_finished;
}

Status StreamHashJoinProbeOperator::set_epoch_finishing(RuntimeState* state) {
    // All input for this epoch has been received
    _is_input_finished = true;
    return Status::OK();
}

Status StreamHashJoinProbeOperator::set_epoch_finished(RuntimeState* state) {
    _is_epoch_finished = true;
    return Status::OK();
}

Status StreamHashJoinProbeOperator::reset_epoch(RuntimeState* state) {
    // Reset for the next epoch
    _is_input_finished = false;
    _is_epoch_finished = false;
    _output_chunk = nullptr;

    // Clear any buffered chunks
    while (!_input_chunks.empty()) {
        _input_chunks.pop();
    }

    // Reset the join state table for the new epoch
    if (_join_state_table) {
        RETURN_IF_ERROR(_join_state_table->reset_epoch(state));
    }

    return Status::OK();
}

Status StreamHashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (!chunk || chunk->is_empty()) {
        return Status::OK();
    }

    // Convert to StreamChunk if necessary
    StreamChunkPtr stream_chunk = ensure_stream_chunk(chunk);
    _input_chunks.push(stream_chunk);

    return Status::OK();
}

StatusOr<ChunkPtr> StreamHashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    // If we have a buffered output chunk, return it
    if (_output_chunk) {
        ChunkPtr result = _output_chunk;
        _output_chunk = nullptr;
        return result;
    }

    // Process input chunks
    while (!_input_chunks.empty()) {
        StreamChunkPtr input = _input_chunks.front();
        _input_chunks.pop();

        auto result = process_chunk_internal(state, input);
        if (!result.ok()) {
            return result.status();
        }

        StreamChunkPtr output = result.value();
        if (output && !output->is_empty()) {
            // Return the first non-empty result
            return std::static_pointer_cast<Chunk>(output);
        }
    }

    // No output available
    if (_is_input_finished) {
        _is_finished = true;
    }

    return nullptr;
}

void StreamHashJoinProbeOperator::set_build_side(ChunkPtr build_side) {
    _build_side = std::move(build_side);
}

void StreamHashJoinProbeOperator::set_join_state_table(JoinStateTablePtr state_table) {
    _join_state_table = std::move(state_table);
}

StreamChunkPtr StreamHashJoinProbeOperator::ensure_stream_chunk(const ChunkPtr& chunk) {
    // Try to cast to StreamChunk first
    auto stream_chunk = std::dynamic_pointer_cast<StreamChunk>(chunk);
    if (stream_chunk && StreamChunkConverter::has_ops_column(stream_chunk)) {
        return stream_chunk;
    }

    // Create ops column with all INSERT operations
    auto ops_column = Int8Column::create();
    int8_t insert_op = static_cast<int8_t>(StreamRowOp::OP_INSERT);
    ops_column->append_value_multiple_times(&insert_op, chunk->num_rows());

    // Create StreamChunk with ops column
    return StreamChunkConverter::make_stream_chunk(chunk, std::move(ops_column));
}

StatusOr<StreamChunkPtr> StreamHashJoinProbeOperator::process_chunk_internal(RuntimeState* state,
                                                                             const StreamChunkPtr& chunk) {
    if (!_build_side) {
        // No build side yet, buffer the chunk or return empty
        return Status::InternalError("Build side not set for StreamHashJoinProbeOperator");
    }

    // Delegate to the underlying StreamJoinOperator
    return _stream_join_operator->process_chunk(state, chunk, _build_side);
}

// ============================================================================
// StreamHashJoinProbeOperatorFactory implementation
// ============================================================================

StreamHashJoinProbeOperatorFactory::StreamHashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                       TJoinOp::type join_type,
                                                                       std::vector<ExprContext*> probe_expr_ctxs,
                                                                       std::vector<ExprContext*> build_expr_ctxs)
        : pipeline::OperatorFactory(id, "stream_hash_join_probe", plan_node_id),
          _join_type(join_type),
          _probe_expr_ctxs(std::move(probe_expr_ctxs)),
          _build_expr_ctxs(std::move(build_expr_ctxs)) {}

pipeline::OperatorPtr StreamHashJoinProbeOperatorFactory::create(int32_t degree_of_parallelism,
                                                                 int32_t driver_sequence) {
    return std::make_shared<StreamHashJoinProbeOperator>(this, _id, _plan_node_id, driver_sequence, _join_type,
                                                         _probe_expr_ctxs, _build_expr_ctxs);
}

Status StreamHashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    // Prepare expression contexts
    RETURN_IF_ERROR(Expr::prepare(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_build_expr_ctxs, state));

    return Status::OK();
}

void StreamHashJoinProbeOperatorFactory::close(RuntimeState* state) {
    Expr::close(_probe_expr_ctxs, state);
    Expr::close(_build_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::stream
