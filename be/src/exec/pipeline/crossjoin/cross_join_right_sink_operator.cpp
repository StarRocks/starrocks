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
        auto build_chunk = _cross_join_context->get_build_chunk(_driver_sequence);
        if (build_chunk == nullptr) {
            _cross_join_context->set_build_chunk(_driver_sequence, chunk);
        } else {
            build_chunk->append(*chunk);
        }
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
