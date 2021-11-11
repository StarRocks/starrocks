// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/set/intersect_build_sink_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> IntersectBuildSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from sink operator");
}

Status IntersectBuildSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    return _intersect_ctx->append_chunk_to_ht(state, chunk, _dst_exprs);
}

Status IntersectBuildSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    return _intersect_ctx->prepare(state, _dst_exprs);
}

Status IntersectBuildSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RowDescriptor row_desc;
    Expr::prepare(_dst_exprs, state, row_desc);
    Expr::open(_dst_exprs, state);

    return Status::OK();
}

void IntersectBuildSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_dst_exprs, state);

    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
