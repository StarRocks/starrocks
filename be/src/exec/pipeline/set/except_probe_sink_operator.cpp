// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/set/except_probe_sink_operator.h"

namespace starrocks::pipeline {

void ExceptProbeSinkOperator::close(RuntimeState* state) {
    _except_ctx->unref(state);
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> ExceptProbeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from sink operator");
}

Status ExceptProbeSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _except_ctx->erase_chunk_from_ht(state, chunk, _dst_exprs);
}

Status ExceptProbeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    Expr::prepare(_dst_exprs, state);
    Expr::open(_dst_exprs, state);

    return Status::OK();
}

void ExceptProbeSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_dst_exprs, state);

    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
