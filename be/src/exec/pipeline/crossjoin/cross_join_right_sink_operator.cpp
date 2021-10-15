// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/crossjoin/cross_join_right_sink_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status CrossJoinRightSinkOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status CrossJoinRightSinkOperator::close(RuntimeState* state) {
    return Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> CrossJoinRightSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "Shouldn't pull chunk from result sink operator";
}

bool CrossJoinRightSinkOperator::need_input() const {
    return !is_finished();
}

Status CrossJoinRightSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (chunk) {
        const size_t row_number = chunk->num_rows();
        if (row_number > 0) {
            DCHECK(*_build_chunk_ptr);
            if ((*_build_chunk_ptr)->num_rows() == 0) {
                *_build_chunk_ptr = chunk;
            } else {
                // merge chunks from right table.
                size_t col_number = chunk->num_columns();
                for (size_t col = 0; col < col_number; ++col) {
                    (*_build_chunk_ptr)
                            ->get_column_by_index(col)
                            ->append(*(chunk->get_column_by_index(col).get()), 0, row_number);
                }
            }
        }
    }

    return Status::OK();
}

void CrossJoinRightSinkOperator::finish(RuntimeState* state) {
    if (!_is_finished) {
        DCHECK(*_build_chunk_ptr);
        // Used to notify cross_join_left_operator.
        *_right_table_complete_ptr = true;
        _is_finished = true;
    }
}

Status CrossJoinRightSinkOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    return Status::OK();
}

void CrossJoinRightSinkOperatorFactory::close(RuntimeState* state) {}

} // namespace starrocks::pipeline
