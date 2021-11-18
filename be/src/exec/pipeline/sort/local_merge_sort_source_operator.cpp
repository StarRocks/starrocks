// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/sort/local_merge_sort_source_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status LocalMergeSortSourceOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(_sort_context->close_one_operator(state));
    return Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> LocalMergeSortSourceOperator::pull_chunk(RuntimeState* state) {
    return _sort_context->pull_chunk();
}

void LocalMergeSortSourceOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
    }
}

void LocalMergeSortSourceOperator::set_finished(RuntimeState* state) {
    _sort_context->set_finished();
}

bool LocalMergeSortSourceOperator::has_output() const {
    return _sort_context->is_partition_sort_finished() && !_sort_context->is_output_finished();
}

bool LocalMergeSortSourceOperator::is_finished() const {
    return _sort_context->is_partition_sort_finished() && _sort_context->is_output_finished();
}

} // namespace starrocks::pipeline
