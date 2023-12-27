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

#include "exec/pipeline/set/except_probe_sink_operator.h"

namespace starrocks::pipeline {

Status ExceptProbeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _except_ctx->incr_prober(_dependency_index);
    RETURN_IF_ERROR(_buffer_state->init(state));
    return Status::OK();
}

void ExceptProbeSinkOperator::close(RuntimeState* state) {
    _buffer_state.reset();
    _except_ctx->unref(state);
    Operator::close(state);
}

StatusOr<ChunkPtr> ExceptProbeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from sink operator");
}

Status ExceptProbeSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _except_ctx->erase_chunk_from_ht(state, chunk, _dst_exprs, _buffer_state.get());
}

Status ExceptProbeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RETURN_IF_ERROR(Expr::prepare(_dst_exprs, state));
    RETURN_IF_ERROR(Expr::open(_dst_exprs, state));

    return Status::OK();
}

void ExceptProbeSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_dst_exprs, state);

    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
