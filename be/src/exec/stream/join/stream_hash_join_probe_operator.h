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

#pragma once

#include <queue>
#include <utility>

#include "column/stream_chunk.h"
#include "exec/pipeline/operator.h"
#include "exec/stream/join/join_state_table.h"
#include "exec/stream/join/stream_join_operator.h"

namespace starrocks::stream {

class StreamHashJoinProbeOperator;
using StreamHashJoinProbeOperatorPtr = std::shared_ptr<StreamHashJoinProbeOperator>;

/**
 * StreamHashJoinProbeOperator is a Pipeline operator that wraps StreamJoinOperator
 * for incremental JOIN computation in the streaming/MV refresh context.
 *
 * Key features:
 * 1. Integrates StreamJoinOperator with the Pipeline execution framework
 * 2. Handles epoch-based execution for streaming scenarios
 * 3. Propagates StreamRowOp (INSERT/DELETE/UPDATE) through the join
 * 4. For OUTER JOINs, detects NULL row changes using JoinStateTable
 *
 * The operator receives probe chunks with ops columns and outputs
 * joined chunks with correctly propagated ops.
 */
class StreamHashJoinProbeOperator : public pipeline::Operator {
public:
    StreamHashJoinProbeOperator(pipeline::OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                int32_t driver_sequence, TJoinOp::type join_type,
                                std::vector<ExprContext*> probe_expr_ctxs, std::vector<ExprContext*> build_expr_ctxs);

    ~StreamHashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    // Epoch management for streaming execution
    bool is_epoch_finished() const override;
    Status set_epoch_finishing(RuntimeState* state) override;
    Status set_epoch_finished(RuntimeState* state) override;
    Status reset_epoch(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    /**
     * Set the build side snapshot for the join.
     * This should be called before processing probe chunks.
     */
    void set_build_side(ChunkPtr build_side);

    /**
     * Set the join state table for tracking match counts (OUTER JOINs).
     */
    void set_join_state_table(JoinStateTablePtr state_table);

private:
    /**
     * Convert a regular chunk to StreamChunk if it doesn't have ops column.
     * Default op is OP_INSERT.
     */
    StreamChunkPtr ensure_stream_chunk(const ChunkPtr& chunk);

    /**
     * Process a chunk through the StreamJoinOperator.
     */
    StatusOr<StreamChunkPtr> process_chunk_internal(RuntimeState* state, const StreamChunkPtr& chunk);

private:
    TJoinOp::type _join_type;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;

    // The underlying stream join operator
    std::unique_ptr<StreamJoinOperator> _stream_join_operator;

    // Join state table for tracking match counts
    JoinStateTablePtr _join_state_table;

    // Build side snapshot for the join
    ChunkPtr _build_side;

    // Buffered input chunks waiting to be processed
    std::queue<StreamChunkPtr> _input_chunks;

    // Buffered output chunk from processing
    StreamChunkPtr _output_chunk;

    // Whether prev operator has no more output
    bool _is_input_finished = false;

    // Whether this operator has finished outputting
    bool _is_finished = false;

    // Epoch management
    bool _is_epoch_finished = false;

    RuntimeState* _runtime_state = nullptr;
};

/**
 * Factory for creating StreamHashJoinProbeOperator instances.
 */
class StreamHashJoinProbeOperatorFactory final : public pipeline::OperatorFactory {
public:
    StreamHashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id, TJoinOp::type join_type,
                                       std::vector<ExprContext*> probe_expr_ctxs,
                                       std::vector<ExprContext*> build_expr_ctxs);

    ~StreamHashJoinProbeOperatorFactory() override = default;

    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    TJoinOp::type _join_type;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;
};

} // namespace starrocks::stream
