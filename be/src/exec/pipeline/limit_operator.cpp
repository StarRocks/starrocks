// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/limit_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
StatusOr<vectorized::ChunkPtr> LimitOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

Status LimitOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _cur_chunk = chunk;
    if (chunk->num_rows() <= _limit) {
        _limit -= chunk->num_rows();
    } else {
        _cur_chunk->set_num_rows(_limit);
        _limit = 0;
    }
    return Status::OK();
}
} // namespace starrocks::pipeline
