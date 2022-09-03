// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/nljoin/nljoin_build_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

void NLJoinBuildOperator::close(RuntimeState* state) {
    auto build_rows = ADD_COUNTER(_unique_metrics, "BuildRows", TUnit::UNIT);
    COUNTER_SET(build_rows, (int64_t)_cross_join_context->num_build_rows());
    auto build_chunks = ADD_COUNTER(_unique_metrics, "BuildChunks", TUnit::UNIT);
    COUNTER_SET(build_chunks, (int64_t)_cross_join_context->num_build_chunks());
    _unique_metrics->add_info_string("NumBuilders", std::to_string(_cross_join_context->get_num_builders()));

    _cross_join_context->unref(state);
    Operator::close(state);
}

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
