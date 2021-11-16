// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/sort/partition_sort_sink_operator.h"

#include <execinfo.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status PartitionSortSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

Status PartitionSortSinkOperator::close(RuntimeState* state) {
    return Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> PartitionSortSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "Shouldn't pull chunk from result sink operator";
}

bool PartitionSortSinkOperator::need_input() const {
    return true;
}

Status PartitionSortSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    vectorized::ChunkPtr materialize_chunk = ChunksSorter::materialize_chunk_before_sort(
            chunk.get(), _materialized_tuple_desc, _sort_exec_exprs, _order_by_types);
    RETURN_IF_ERROR(_chunks_sorter->update(state, materialize_chunk));
    return Status::OK();
}

void PartitionSortSinkOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _chunks_sorter->finish(state);

        // Current partition sort is ended, and
        // the last call will drive LocalMergeSortSourceOperator to work.
        _sort_context->finish_partition(_chunks_sorter->get_partition_rows());
        _is_finished = true;
    }
}

Status PartitionSortSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, _parent_node_row_desc, _parent_node_child_row_desc));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));
    return Status::OK();
}

void PartitionSortSinkOperatorFactory::close(RuntimeState* state) {
    _sort_exec_exprs.close(state);
}

} // namespace starrocks::pipeline
