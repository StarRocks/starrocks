// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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

void LocalMergeSortSourceOperator::close(RuntimeState* state) {
    _sort_context->unref(state);
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> LocalMergeSortSourceOperator::pull_chunk(RuntimeState* state) {
    return _sort_context->pull_chunk();
}

Status LocalMergeSortSourceOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status LocalMergeSortSourceOperator::set_finished(RuntimeState* state) {
    return _sort_context->set_finished();
}

bool LocalMergeSortSourceOperator::has_output() const {
    return _sort_context->is_partition_sort_finished() && !_sort_context->is_output_finished();
}

bool LocalMergeSortSourceOperator::is_finished() const {
    return _sort_context->is_partition_sort_finished() && _sort_context->is_output_finished();
}
OperatorPtr LocalMergeSortSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto sort_context = _sort_context_factory->create(driver_sequence);
    return std::make_shared<LocalMergeSortSourceOperator>(this, _id, _plan_node_id, sort_context.get());
}

} // namespace starrocks::pipeline
