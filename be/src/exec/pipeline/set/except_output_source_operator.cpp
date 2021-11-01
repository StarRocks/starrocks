// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/set/except_output_source_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> ExceptOutputSourceOperator::pull_chunk(RuntimeState* state) {
    return _except_ctx->pull_chunk(state);
}

Status ExceptOutputSourceOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(_except_ctx->close(state));
    return Operator::close(state);
}

void ExceptOutputSourceOperatorFactory::close(RuntimeState* state) {
    SourceOperatorFactory::close(state);
}

} // namespace starrocks::pipeline
