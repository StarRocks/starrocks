// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/crossjoin/nljoin_build_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "runtime/current_thread.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> NLJoinBuildOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from cross join right sink operator");
}

Status NLJoinBuildOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (chunk->num_rows() > 0) {
        _cross_join_context->append_build_chunk(_driver_sequence, chunk);
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
