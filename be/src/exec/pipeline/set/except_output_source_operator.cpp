// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/set/except_output_source_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> ExceptOutputSourceOperator::pull_chunk(RuntimeState* state) {
    return _except_ctx->pull_chunk(state);
}

void ExceptOutputSourceOperator::close(RuntimeState* state) {
    _except_ctx->unref(state);
    Operator::close(state);
}

void ExceptOutputSourceOperatorFactory::close(RuntimeState* state) {
    SourceOperatorFactory::close(state);
}

} // namespace starrocks::pipeline
