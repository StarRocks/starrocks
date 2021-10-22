// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/select_operator.h"

#include "column/chunk.h"
#include "exec/exec_node.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status SelectOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status SelectOperator::close(RuntimeState* state) {
    Operator::close(state);
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> SelectOperator::pull_chunk(RuntimeState* state) {
    ExecNode::eval_conjuncts(_conjunct_ctxs, _curr_chunk.get());
    return std::move(_curr_chunk);
}

bool SelectOperator::need_input() const {
    return !_curr_chunk;
}

Status SelectOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _curr_chunk = chunk;
    return Status::OK();
}

Status SelectOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc, mem_tracker));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

void SelectOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
}

} // namespace starrocks::pipeline
