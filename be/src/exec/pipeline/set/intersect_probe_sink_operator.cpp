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

#include "exec/pipeline/set/intersect_probe_sink_operator.h"

namespace starrocks::pipeline {

void IntersectProbeSinkOperator::close(RuntimeState* state) {
    _intersect_ctx->unref(state);
    Operator::close(state);
}

StatusOr<ChunkPtr> IntersectProbeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from sink operator");
}

Status IntersectProbeSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _intersect_ctx->refine_chunk_from_ht(state, chunk, _dst_exprs, _dependency_index + 1);
}

Status IntersectProbeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    Expr::prepare(_dst_exprs, state);
    Expr::open(_dst_exprs, state);

    return Status::OK();
}

void IntersectProbeSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_dst_exprs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
