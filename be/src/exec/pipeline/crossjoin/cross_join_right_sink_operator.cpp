// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/crossjoin/cross_join_right_sink_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/pipeline/crossjoin/nljoin_build_operator.h"
#include "runtime/current_thread.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> CrossJoinRightSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from cross join right sink operator");
}

Status CrossJoinRightSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (chunk->num_rows() > 0) {
        _cross_join_context->append_build_chunk(_driver_sequence, chunk);
    }

    return Status::OK();
}

OperatorPtr CrossJoinRightSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<NLJoinBuildOperator>(this, _id, _plan_node_id, driver_sequence, _cross_join_context);
}

} // namespace starrocks::pipeline
