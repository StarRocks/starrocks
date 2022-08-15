// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/crossjoin/cross_join_right_sink_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "runtime/current_thread.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> CrossJoinRightSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from cross join right sink operator");
}

Status CrossJoinRightSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    const size_t row_number = chunk->num_rows();
    if (row_number > 0) {
        if (_cross_join_context->get_build_chunk(_driver_sequence) == nullptr) {
            _cross_join_context->set_build_chunk(_driver_sequence, chunk);
        } else {
            // merge chunks from right table.
            size_t col_number = chunk->num_columns();
            for (size_t col = 0; col < col_number; ++col) {
                TRY_CATCH_BAD_ALLOC(_cross_join_context->get_build_chunk(_driver_sequence)
                                            ->get_column_by_index(col)
                                            ->append(*(chunk->get_column_by_index(col).get()), 0, row_number));
            }
        }
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
