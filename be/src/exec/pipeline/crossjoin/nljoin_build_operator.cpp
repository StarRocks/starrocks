// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/crossjoin/nljoin_build_operator.h"

#include "column/chunk.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> NLJoinBuildOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from cross join right sink operator");
}

Status NLJoinBuildOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    // Used to notify cross_join_left_operator.
    RETURN_IF_ERROR(_cross_join_context->finish_one_right_sinker(state));
    return Status::OK();
}

Status NLJoinBuildOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _cross_join_context->append_build_chunk(_driver_sequence, chunk);

    return Status::OK();
}

} // namespace starrocks::pipeline
