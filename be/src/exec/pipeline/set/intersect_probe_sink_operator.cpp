// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/set/intersect_probe_sink_operator.h"

namespace starrocks::pipeline {

void IntersectProbeSinkOperator::close(RuntimeState* state) {
    _intersect_ctx->unref(state);
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> IntersectProbeSinkOperator::pull_chunk(RuntimeState* state) {
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
