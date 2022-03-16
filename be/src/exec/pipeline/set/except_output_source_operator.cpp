// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/set/except_output_source_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> ExceptOutputSourceOperator::pull_chunk(RuntimeState* state) {
    return _except_ctx->pull_chunk(state);
}

Status ExceptOutputSourceOperator::close(RuntimeState* state) {
    _except_ctx->unref(state);
    return Operator::close(state);
}

Status ExceptOutputSourceOperatorFactory::close(RuntimeState* state) {
    return SourceOperatorFactory::close(state);
}

} // namespace starrocks::pipeline
