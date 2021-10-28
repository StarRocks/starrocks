// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/set/except_erase_sink_operator.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> ExceptEraseSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from sink operator");
}

Status ExceptEraseSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _except_ctx->erase_chunk_from_ht(state, chunk, _dst_exprs);
}

Status ExceptEraseSinkOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state, mem_tracker));

    RowDescriptor row_desc;
    Expr::prepare(_dst_exprs, state, row_desc, mem_tracker);
    Expr::open(_dst_exprs, state);

    return Status::OK();
}

void ExceptEraseSinkOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);

    Expr::close(_dst_exprs, state);
}

} // namespace starrocks::pipeline
