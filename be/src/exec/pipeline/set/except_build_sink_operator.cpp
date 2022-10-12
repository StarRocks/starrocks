// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/set/except_build_sink_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> ExceptBuildSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from sink operator");
}

Status ExceptBuildSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _except_ctx->append_chunk_to_ht(state, chunk, _dst_exprs);
}

Status ExceptBuildSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    return _except_ctx->prepare(state, _dst_exprs);
}

void ExceptBuildSinkOperator::close(RuntimeState* state) {
    _except_ctx->unref(state);
    Operator::close(state);
}

Status ExceptBuildSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    Expr::prepare(_dst_exprs, state);
    Expr::open(_dst_exprs, state);

    return Status::OK();
}

void ExceptBuildSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_dst_exprs, state);

    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
