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

#include "exec/stream/join/stream_join_operator.h"

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "util/raw_container.h"

namespace starrocks::stream {

StreamJoinOperator::StreamJoinOperator(TJoinOp::type join_type, std::vector<ExprContext*> probe_expr_ctxs,
                                       std::vector<ExprContext*> build_expr_ctxs)
        : _join_type(join_type),
          _probe_expr_ctxs(std::move(probe_expr_ctxs)),
          _build_expr_ctxs(std::move(build_expr_ctxs)) {}

Status StreamJoinOperator::prepare(RuntimeState* state) {
    _runtime_state = state;
    return Status::OK();
}

Status StreamJoinOperator::open(RuntimeState* state) {
    return Status::OK();
}

void StreamJoinOperator::close(RuntimeState* state) {
    _join_state_table.reset();
}

StatusOr<StreamChunkPtr> StreamJoinOperator::process_chunk(RuntimeState* state, const StreamChunkPtr& probe_chunk,
                                                           const ChunkPtr& build_side_snapshot) {
    if (!probe_chunk || probe_chunk->is_empty()) {
        return nullptr;
    }

    switch (_join_type) {
    case TJoinOp::INNER_JOIN:
    case TJoinOp::CROSS_JOIN:
        return process_inner_join(state, probe_chunk, build_side_snapshot);
    case TJoinOp::LEFT_OUTER_JOIN:
        return process_left_outer_join(state, probe_chunk, build_side_snapshot);
    case TJoinOp::RIGHT_OUTER_JOIN:
        return process_right_outer_join(state, probe_chunk, build_side_snapshot);
    case TJoinOp::FULL_OUTER_JOIN:
        return process_full_outer_join(state, probe_chunk, build_side_snapshot);
    default:
        return Status::NotSupported("Unsupported join type for StreamJoin: " + std::to_string(_join_type));
    }
}

StatusOr<StreamChunkPtr> StreamJoinOperator::process_inner_join(RuntimeState* state, const StreamChunkPtr& probe_chunk,
                                                                const ChunkPtr& build_side) {
    if (!build_side || build_side->is_empty()) {
        return nullptr;
    }

    // Get ops from probe chunk
    const StreamRowOp* probe_ops = StreamChunkConverter::ops(probe_chunk);
    if (probe_ops == nullptr) {
        return Status::InternalError("StreamChunk must have ops column for StreamJoin");
    }

    size_t probe_rows = probe_chunk->num_rows();
    size_t build_rows = build_side->num_rows();

    // Prepare output chunk - for simplicity, we do a nested loop join here
    // In production, this should use hash join for efficiency
    auto output_chunk = std::make_shared<Chunk>();

    // Output ops column
    auto output_ops = Int8Column::create();

    // Evaluate probe keys
    Columns probe_keys;
    for (auto* ctx : _probe_expr_ctxs) {
        ASSIGN_OR_RETURN(auto col, ctx->evaluate(probe_chunk.get()));
        probe_keys.push_back(col);
    }

    // Evaluate build keys
    Columns build_keys;
    for (auto* ctx : _build_expr_ctxs) {
        ASSIGN_OR_RETURN(auto col, ctx->evaluate(build_side.get()));
        build_keys.push_back(col);
    }

    // Simple nested loop join with key matching
    // For INNER JOIN, output op = probe op (INSERT/DELETE propagates)
    for (size_t probe_idx = 0; probe_idx < probe_rows; ++probe_idx) {
        StreamRowOp probe_op = probe_ops[probe_idx];

        for (size_t build_idx = 0; build_idx < build_rows; ++build_idx) {
            // Check if keys match
            bool keys_match = true;
            for (size_t key_idx = 0; key_idx < probe_keys.size() && keys_match; ++key_idx) {
                if (probe_keys[key_idx]->compare_at(probe_idx, build_idx, *build_keys[key_idx], -1) != 0) {
                    keys_match = false;
                }
            }

            if (keys_match) {
                // Append probe columns
                for (size_t col_idx = 0; col_idx < probe_chunk->num_columns(); ++col_idx) {
                    auto& probe_col = probe_chunk->get_column_by_index(col_idx);
                    if (output_chunk->num_columns() <= col_idx) {
                        output_chunk->append_column(probe_col->clone_empty(), col_idx);
                    }
                    output_chunk->get_column_by_index(col_idx)->append(*probe_col, probe_idx, 1);
                }

                // Append build columns
                size_t base_col = probe_chunk->num_columns();
                for (size_t col_idx = 0; col_idx < build_side->num_columns(); ++col_idx) {
                    auto& build_col = build_side->get_column_by_index(col_idx);
                    size_t output_col_idx = base_col + col_idx;
                    if (output_chunk->num_columns() <= output_col_idx) {
                        output_chunk->append_column(build_col->clone_empty(), output_col_idx);
                    }
                    output_chunk->get_column_by_index(output_col_idx)->append(*build_col, build_idx, 1);
                }

                // Output op is the same as probe op for INNER JOIN
                output_ops->append(static_cast<int8_t>(probe_op));
            }
        }
    }

    if (output_chunk->is_empty()) {
        return nullptr;
    }

    // Attach ops column to output chunk
    return StreamChunkConverter::make_stream_chunk(output_chunk, std::move(output_ops));
}

StatusOr<StreamChunkPtr> StreamJoinOperator::process_left_outer_join(RuntimeState* state,
                                                                     const StreamChunkPtr& probe_chunk,
                                                                     const ChunkPtr& build_side) {
    // Get ops from probe chunk
    const StreamRowOp* probe_ops = StreamChunkConverter::ops(probe_chunk);
    if (probe_ops == nullptr) {
        return Status::InternalError("StreamChunk must have ops column for StreamJoin");
    }

    size_t probe_rows = probe_chunk->num_rows();
    size_t build_rows = build_side ? build_side->num_rows() : 0;
    size_t probe_col_count = probe_chunk->num_columns();
    size_t build_col_count = build_side ? build_side->num_columns() : 0;

    // Prepare output chunk
    auto output_chunk = std::make_shared<Chunk>();
    auto output_ops = Int8Column::create();

    // Evaluate probe keys
    Columns probe_keys;
    for (auto* ctx : _probe_expr_ctxs) {
        ASSIGN_OR_RETURN(auto col, ctx->evaluate(probe_chunk.get()));
        probe_keys.push_back(col);
    }

    // Evaluate build keys (if build side exists)
    Columns build_keys;
    if (build_side && !build_side->is_empty()) {
        for (auto* ctx : _build_expr_ctxs) {
            ASSIGN_OR_RETURN(auto col, ctx->evaluate(build_side.get()));
            build_keys.push_back(col);
        }
    }

    // Process each probe row
    for (size_t probe_idx = 0; probe_idx < probe_rows; ++probe_idx) {
        StreamRowOp probe_op = probe_ops[probe_idx];
        std::string probe_key_str = serialize_keys(probe_keys, probe_idx);

        bool has_match = false;
        int64_t old_match_count = _left_match_state.get_match_count(probe_key_str);

        // Find matches in build side
        for (size_t build_idx = 0; build_idx < build_rows; ++build_idx) {
            bool keys_match = true;
            for (size_t key_idx = 0; key_idx < probe_keys.size() && keys_match; ++key_idx) {
                if (probe_keys[key_idx]->compare_at(probe_idx, build_idx, *build_keys[key_idx], -1) != 0) {
                    keys_match = false;
                }
            }

            if (keys_match) {
                has_match = true;

                // Output matched row
                for (size_t col_idx = 0; col_idx < probe_col_count; ++col_idx) {
                    auto& probe_col = probe_chunk->get_column_by_index(col_idx);
                    if (output_chunk->num_columns() <= col_idx) {
                        output_chunk->append_column(probe_col->clone_empty(), col_idx);
                    }
                    output_chunk->get_column_by_index(col_idx)->append(*probe_col, probe_idx, 1);
                }

                for (size_t col_idx = 0; col_idx < build_col_count; ++col_idx) {
                    auto& build_col = build_side->get_column_by_index(col_idx);
                    size_t output_col_idx = probe_col_count + col_idx;
                    if (output_chunk->num_columns() <= output_col_idx) {
                        output_chunk->append_column(build_col->clone_empty(), output_col_idx);
                    }
                    output_chunk->get_column_by_index(output_col_idx)->append(*build_col, build_idx, 1);
                }

                // For INSERT: this is a new matched row
                // For DELETE: this matched row is being removed
                output_ops->append(static_cast<int8_t>(probe_op));

                // Update match count based on op
                if (probe_op == StreamRowOp::OP_INSERT || probe_op == StreamRowOp::OP_UPDATE_AFTER) {
                    _left_match_state.increment_match_count(probe_key_str);
                } else if (probe_op == StreamRowOp::OP_DELETE || probe_op == StreamRowOp::OP_UPDATE_BEFORE) {
                    _left_match_state.decrement_match_count(probe_key_str);
                }
            }
        }

        int64_t new_match_count = _left_match_state.get_match_count(probe_key_str);

        // Handle NULL row changes for LEFT OUTER JOIN
        if (!has_match) {
            // No match found - output NULL-padded row
            // For INSERT: add a new NULL-padded row
            // For DELETE: remove an existing NULL-padded row

            // Output probe columns
            for (size_t col_idx = 0; col_idx < probe_col_count; ++col_idx) {
                auto& probe_col = probe_chunk->get_column_by_index(col_idx);
                if (output_chunk->num_columns() <= col_idx) {
                    output_chunk->append_column(probe_col->clone_empty(), col_idx);
                }
                output_chunk->get_column_by_index(col_idx)->append(*probe_col, probe_idx, 1);
            }

            // Output NULL columns for build side
            for (size_t col_idx = 0; col_idx < build_col_count; ++col_idx) {
                size_t output_col_idx = probe_col_count + col_idx;
                if (output_chunk->num_columns() <= output_col_idx) {
                    // Create nullable column
                    auto& build_col = build_side->get_column_by_index(col_idx);
                    auto nullable_col = NullableColumn::create(build_col->clone_empty(), NullColumn::create());
                    output_chunk->append_column(nullable_col, output_col_idx);
                }
                // Append NULL
                output_chunk->get_column_by_index(output_col_idx)->append_nulls(1);
            }

            output_ops->append(static_cast<int8_t>(probe_op));
        } else if (old_match_count == 0 && new_match_count > 0) {
            // Transition from unmatched to matched
            // Need to retract the old NULL-padded row
            // This happens when a right INSERT causes a previously unmatched left row to now match
            // Output: DELETE(left + NULL)
            // Note: The INSERT(left + right) was already output above

            // This case is handled in process_right_delta() when right side has changes
        } else if (old_match_count > 0 && new_match_count == 0) {
            // Transition from matched to unmatched
            // Need to emit a new NULL-padded row
            // This happens when a right DELETE causes a previously matched left row to now be unmatched
            // Output: INSERT(left + NULL)
            // Note: The DELETE(left + right) was already output above

            // Output probe columns with INSERT op for the new NULL-padded row
            for (size_t col_idx = 0; col_idx < probe_col_count; ++col_idx) {
                auto& probe_col = probe_chunk->get_column_by_index(col_idx);
                if (output_chunk->num_columns() <= col_idx) {
                    output_chunk->append_column(probe_col->clone_empty(), col_idx);
                }
                output_chunk->get_column_by_index(col_idx)->append(*probe_col, probe_idx, 1);
            }

            // Output NULL columns for build side
            for (size_t col_idx = 0; col_idx < build_col_count; ++col_idx) {
                size_t output_col_idx = probe_col_count + col_idx;
                if (output_chunk->num_columns() <= output_col_idx) {
                    auto& build_col = build_side->get_column_by_index(col_idx);
                    auto nullable_col = NullableColumn::create(build_col->clone_empty(), NullColumn::create());
                    output_chunk->append_column(nullable_col, output_col_idx);
                }
                output_chunk->get_column_by_index(output_col_idx)->append_nulls(1);
            }

            // This is a new NULL-padded row, so INSERT
            output_ops->append(static_cast<int8_t>(StreamRowOp::OP_INSERT));
        }
    }

    if (output_chunk->is_empty()) {
        return nullptr;
    }

    return StreamChunkConverter::make_stream_chunk(output_chunk, std::move(output_ops));
}

StatusOr<StreamChunkPtr> StreamJoinOperator::process_right_outer_join(RuntimeState* state,
                                                                      const StreamChunkPtr& probe_chunk,
                                                                      const ChunkPtr& build_side) {
    // RIGHT OUTER JOIN is symmetric to LEFT OUTER JOIN
    // Swap the roles of probe and build, then apply LEFT OUTER JOIN logic
    // For simplicity, we implement a similar logic but track right side match counts

    // Similar implementation to process_left_outer_join but with swapped roles
    // ... (implementation follows same pattern)

    return Status::NotSupported("RIGHT OUTER JOIN StreamJoin not yet fully implemented");
}

StatusOr<StreamChunkPtr> StreamJoinOperator::process_full_outer_join(RuntimeState* state,
                                                                     const StreamChunkPtr& probe_chunk,
                                                                     const ChunkPtr& build_side) {
    // FULL OUTER JOIN combines LEFT and RIGHT OUTER JOIN logic
    // Need to track match counts for both sides

    // ... (implementation combines both LEFT and RIGHT logic)

    return Status::NotSupported("FULL OUTER JOIN StreamJoin not yet fully implemented");
}

std::string StreamJoinOperator::serialize_keys(const Columns& key_columns, size_t row_idx) {
    std::string result;
    for (const auto& col : key_columns) {
        // Simple serialization - in production use more efficient method
        auto datum = col->get(row_idx);
        result += datum.to_string();
        result += "|";
    }
    return result;
}

bool StreamJoinOperator::update_match_count(const std::string& key, StreamRowOp op, int64_t* old_count,
                                            int64_t* new_count) {
    *old_count = _left_match_state.get_match_count(key);

    if (op == StreamRowOp::OP_INSERT || op == StreamRowOp::OP_UPDATE_AFTER) {
        _left_match_state.increment_match_count(key);
    } else if (op == StreamRowOp::OP_DELETE || op == StreamRowOp::OP_UPDATE_BEFORE) {
        _left_match_state.decrement_match_count(key);
    }

    *new_count = _left_match_state.get_match_count(key);

    // Return true if there's a NULL row change
    return (*old_count == 0 && *new_count > 0) || (*old_count > 0 && *new_count == 0);
}

void StreamJoinOperator::append_null_row(ChunkPtr& output, const Columns& non_null_columns, size_t row_idx,
                                         bool null_on_left, size_t left_col_count, size_t right_col_count) {
    if (null_on_left) {
        // Append NULLs for left columns
        for (size_t i = 0; i < left_col_count; ++i) {
            output->get_column_by_index(i)->append_nulls(1);
        }
        // Append actual values for right columns
        for (size_t i = 0; i < right_col_count; ++i) {
            output->get_column_by_index(left_col_count + i)->append(*non_null_columns[i], row_idx, 1);
        }
    } else {
        // Append actual values for left columns
        for (size_t i = 0; i < left_col_count; ++i) {
            output->get_column_by_index(i)->append(*non_null_columns[i], row_idx, 1);
        }
        // Append NULLs for right columns
        for (size_t i = 0; i < right_col_count; ++i) {
            output->get_column_by_index(left_col_count + i)->append_nulls(1);
        }
    }
}

} // namespace starrocks::stream
