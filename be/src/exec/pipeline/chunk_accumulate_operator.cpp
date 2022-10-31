// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/chunk_accumulate_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status ChunkAccumulateOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _acc.push(chunk);
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ChunkAccumulateOperator::pull_chunk(RuntimeState*) {
    return std::move(_acc.pull());
}

Status ChunkAccumulateOperator::set_finishing(RuntimeState* state) {
    _acc.finalize();
    return Status::OK();
}

Status ChunkAccumulateOperator::set_finished(RuntimeState*) {
    _acc.finalize();
    _acc.reset();

    return Status::OK();
}

Status ChunkAccumulateOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _acc.reset();

    return Status::OK();
}

} // namespace starrocks
