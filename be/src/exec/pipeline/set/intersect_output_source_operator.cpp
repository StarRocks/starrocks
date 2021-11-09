// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/set/intersect_output_source_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> IntersectOutputSourceOperator::pull_chunk(RuntimeState* state) {
    return _intersect_ctx->pull_chunk(state);
}

Status IntersectOutputSourceOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(_intersect_ctx->close(state));
    return Operator::close(state);
}

void IntersectOutputSourceOperatorFactory::close(RuntimeState* state) {
    SourceOperatorFactory::close(state);
}

} // namespace starrocks::pipeline