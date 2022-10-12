// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/set/intersect_output_source_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> IntersectOutputSourceOperator::pull_chunk(RuntimeState* state) {
    return _intersect_ctx->pull_chunk(state);
}

void IntersectOutputSourceOperator::close(RuntimeState* state) {
    _intersect_ctx->unref(state);
    Operator::close(state);
}

void IntersectOutputSourceOperatorFactory::close(RuntimeState* state) {
    SourceOperatorFactory::close(state);
}

} // namespace starrocks::pipeline
