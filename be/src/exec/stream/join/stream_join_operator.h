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

#include <memory>
#include <unordered_map>

#include "column/stream_chunk.h"
#include "common/statusor.h"
#include "exec/pipeline/operator.h"
#include "exec/stream/state/state_table.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks::stream {

/**
 * JoinMatchState tracks the match count for each left row in OUTER JOINs.
 * This is essential for detecting NULL row changes:
 * - When match_count goes from 0 -> 1: need to retract the NULL-padded row
 * - When match_count goes from 1 -> 0: need to emit a new NULL-padded row
 */
struct JoinMatchState {
    // Key: serialized left key bytes
    // Value: match count for this left key
    std::unordered_map<std::string, int64_t> match_counts;

    int64_t get_match_count(const std::string& key) const {
        auto it = match_counts.find(key);
        return it != match_counts.end() ? it->second : 0;
    }

    void increment_match_count(const std::string& key) { match_counts[key]++; }

    void decrement_match_count(const std::string& key) {
        auto it = match_counts.find(key);
        if (it != match_counts.end()) {
            it->second--;
            if (it->second <= 0) {
                match_counts.erase(it);
            }
        }
    }
};

/**
 * StreamJoinOperator handles incremental JOIN computation for Incremental MV.
 *
 * Key features:
 * 1. Processes StreamChunks with ops column (INSERT/DELETE/UPDATE)
 * 2. Propagates ops through the join correctly
 * 3. For OUTER JOINs, tracks match counts to detect NULL row changes
 *
 * Supported Join Types:
 * - INNER JOIN: Direct op propagation
 * - LEFT OUTER JOIN: Tracks left side match counts for NULL row changes
 * - RIGHT OUTER JOIN: Symmetric to LEFT OUTER JOIN
 * - FULL OUTER JOIN: Tracks both sides
 *
 * Join incremental formulas:
 * - INNER JOIN: Δ(A ⋈ B) = (A_from ⋈ ΔB) ∪ (ΔA ⋈ B_to)
 * - LEFT OUTER JOIN: Δ(A ⟕ B) = (ΔA ⟕ B_to) ∪ (A_from ⋈ ΔB) ∪ NullRowChanges
 */
class StreamJoinOperator {
public:
    StreamJoinOperator(TJoinOp::type join_type, std::vector<ExprContext*> probe_expr_ctxs,
                       std::vector<ExprContext*> build_expr_ctxs);

    ~StreamJoinOperator() = default;

    Status prepare(RuntimeState* state);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);

    /**
     * Process a probe chunk with ops column.
     * The output chunk will have the correct ops based on:
     * - Input ops propagation
     * - NULL row change detection (for OUTER JOINs)
     */
    StatusOr<StreamChunkPtr> process_chunk(RuntimeState* state, const StreamChunkPtr& probe_chunk,
                                           const ChunkPtr& build_side_snapshot);

    /**
     * Set the state table for tracking join state (match counts).
     * Required for OUTER JOINs to correctly handle NULL row changes.
     */
    void set_join_state_table(std::unique_ptr<StateTable> state_table) { _join_state_table = std::move(state_table); }

    TJoinOp::type join_type() const { return _join_type; }

private:
    /**
     * Process INNER JOIN: directly propagate ops from probe side.
     * For each (probe_row, probe_op) that matches build_row:
     *   output (probe_row ⋈ build_row, probe_op)
     */
    StatusOr<StreamChunkPtr> process_inner_join(RuntimeState* state, const StreamChunkPtr& probe_chunk,
                                                const ChunkPtr& build_side);

    /**
     * Process LEFT OUTER JOIN: handle both matched rows and NULL row changes.
     *
     * For matched rows: same as INNER JOIN
     * For NULL row changes when right side changes:
     *   - Right INSERT making 0->1 match: DELETE(left+NULL), INSERT(left+right)
     *   - Right DELETE making 1->0 match: DELETE(left+right), INSERT(left+NULL)
     */
    StatusOr<StreamChunkPtr> process_left_outer_join(RuntimeState* state, const StreamChunkPtr& probe_chunk,
                                                     const ChunkPtr& build_side);

    /**
     * Process RIGHT OUTER JOIN: symmetric to LEFT OUTER JOIN.
     */
    StatusOr<StreamChunkPtr> process_right_outer_join(RuntimeState* state, const StreamChunkPtr& probe_chunk,
                                                      const ChunkPtr& build_side);

    /**
     * Process FULL OUTER JOIN: combine LEFT and RIGHT logic.
     */
    StatusOr<StreamChunkPtr> process_full_outer_join(RuntimeState* state, const StreamChunkPtr& probe_chunk,
                                                     const ChunkPtr& build_side);

    /**
     * Serialize join keys to a string for state table lookup.
     */
    std::string serialize_keys(const Columns& key_columns, size_t row_idx);

    /**
     * Update match count in state table and detect NULL row changes.
     * Returns true if this update causes a NULL row change.
     */
    bool update_match_count(const std::string& key, StreamRowOp op, int64_t* old_count, int64_t* new_count);

    /**
     * Create a NULL-padded row for OUTER JOIN output.
     */
    void append_null_row(ChunkPtr& output, const Columns& non_null_columns, size_t row_idx, bool null_on_left,
                         size_t left_col_count, size_t right_col_count);

private:
    TJoinOp::type _join_type;
    std::vector<ExprContext*> _probe_expr_ctxs;
    std::vector<ExprContext*> _build_expr_ctxs;

    // State table for tracking join match counts (for OUTER JOINs)
    std::unique_ptr<StateTable> _join_state_table;

    // In-memory match state (can be backed by state table for persistence)
    JoinMatchState _left_match_state;
    JoinMatchState _right_match_state;

    RuntimeState* _runtime_state = nullptr;
};

using StreamJoinOperatorPtr = std::shared_ptr<StreamJoinOperator>;

} // namespace starrocks::stream
