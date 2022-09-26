// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/lookupjoin/lookup_join_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "exprs/vectorized/column_ref.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

void LookupJoinOperator::close(RuntimeState* state) {
    _lookup_join_context->unref(state);
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> LookupJoinOperator::pull_chunk(RuntimeState* state) {
    VLOG(1) << "StreamJoin: mock output: ";
    if (!_other_join_conjuncts.empty()) {
        RETURN_IF_ERROR(eval_conjuncts(_other_join_conjuncts, _cur_chunk.get(), nullptr));
    }
    for (size_t i = 0; i < _cur_chunk->num_rows(); i++) {
        VLOG(2) << "StreamJoin: mock output: " << _cur_chunk->debug_row(i);
    }
    return std::move(_cur_chunk);
}

Status LookupJoinOperator::set_finishing(RuntimeState* state) {
    VLOG(1)<<"lookup join finished.";
    _is_finished = true;
    return Status::OK();
}

Status LookupJoinOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _cur_chunk = chunk;
    return Status::OK();
}

} // namespace starrocks::pipeline
