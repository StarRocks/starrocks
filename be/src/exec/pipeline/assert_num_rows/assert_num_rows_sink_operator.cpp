// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/assert_num_rows/assert_num_rows_sink_operator.h"

#include "column/chunk.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status AssertNumRowsSinkOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status AssertNumRowsSinkOperator::close(RuntimeState* state) {
    return Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> AssertNumRowsSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "Shouldn't pull chunk from result sink operator";
}

bool AssertNumRowsSinkOperator::need_input() const {
    return !_is_finished;
}

Status AssertNumRowsSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _assert_num_rows_context->add_rows(chunk->num_rows());
    _assert_num_rows_context->add_chunk(chunk);
    return Status::OK();
}

void AssertNumRowsSinkOperator::finish(RuntimeState* state) {
    if (!_is_finished) {
        _assert_num_rows_context->set_data_complete();
        _is_finished = true;
    }
}

Status AssertNumRowsSinkOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    return Status::OK();
}

void AssertNumRowsSinkOperatorFactory::close(RuntimeState* state) {}

} // namespace starrocks::pipeline
