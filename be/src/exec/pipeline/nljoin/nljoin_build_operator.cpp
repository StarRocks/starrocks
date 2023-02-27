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

#include "exec/pipeline/nljoin/nljoin_build_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

Status NLJoinBuildOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _cross_join_context->incr_builder();
    return Status::OK();
}

void NLJoinBuildOperator::close(RuntimeState* state) {
    auto build_rows = ADD_COUNTER(_unique_metrics, "BuildRows", TUnit::UNIT);
    COUNTER_SET(build_rows, (int64_t)_cross_join_context->num_build_rows());
    auto build_chunks = ADD_COUNTER(_unique_metrics, "BuildChunks", TUnit::UNIT);
    COUNTER_SET(build_chunks, (int64_t)_cross_join_context->num_build_chunks());
    _unique_metrics->add_info_string("NumBuilders", std::to_string(_cross_join_context->get_num_builders()));

    _cross_join_context->unref(state);
    Operator::close(state);
}

StatusOr<ChunkPtr> NLJoinBuildOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from cross join right sink operator");
}

Status NLJoinBuildOperator::set_finishing(RuntimeState* state) {
    DeferOp op([this]() { _is_finished = true; });
    // Used to notify cross_join_left_operator.
    RETURN_IF_ERROR(_cross_join_context->finish_one_right_sinker(state));
    return Status::OK();
}

Status NLJoinBuildOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _num_rows += chunk->num_rows();
    _cross_join_context->append_build_chunk(_driver_sequence, chunk);

    return Status::OK();
}

size_t NLJoinBuildOperator::output_amplification_factor() const {
    return _num_rows;
}

Operator::OutputAmplificationType NLJoinBuildOperator::intra_pipeline_amplification_type() const {
    return Operator::OutputAmplificationType::ADD;
}

} // namespace starrocks::pipeline
