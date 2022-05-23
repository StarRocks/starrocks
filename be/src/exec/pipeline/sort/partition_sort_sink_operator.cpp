// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/sort/partition_sort_sink_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/pipeline/sort/sort_context.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_heap_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/expr.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status PartitionSortSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _chunks_sorter->setup_runtime(_unique_metrics.get());
    return Status::OK();
}

void PartitionSortSinkOperator::close(RuntimeState* state) {
    _sort_context->unref(state);
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> PartitionSortSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "Shouldn't pull chunk from result sink operator";
}

Status PartitionSortSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    auto materialize_chunk = ChunksSorter::materialize_chunk_before_sort(chunk.get(), _materialized_tuple_desc,
                                                                         _sort_exec_exprs, _order_by_types);
    RETURN_IF_ERROR(materialize_chunk);
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_chunks_sorter->update(state, materialize_chunk.value())));
    return Status::OK();
}

Status PartitionSortSinkOperator::set_finishing(RuntimeState* state) {
    // skip sorting if cancelled
    if (state->is_cancelled()) {
        _is_finished = true;
        return Status::Cancelled("runtime state is cancelled");
    }
    RETURN_IF_ERROR(_chunks_sorter->finish(state));

    // Current partition sort is ended, and
    // the last call will drive LocalMergeSortSourceOperator to work.
    _sort_context->finish_partition(_chunks_sorter->get_output_rows());
    _is_finished = true;
    return Status::OK();
}

Status PartitionSortSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, _parent_node_row_desc, _parent_node_child_row_desc));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));
    RETURN_IF_ERROR(Expr::prepare(_analytic_partition_exprs, state));
    RETURN_IF_ERROR(Expr::open(_analytic_partition_exprs, state));
    return Status::OK();
}

OperatorPtr PartitionSortSinkOperatorFactory::create(int32_t dop, int32_t driver_sequence) {
    std::shared_ptr<ChunksSorter> chunks_sorter;
    if (_limit >= 0) {
        if (_topn_type == TTopNType::ROW_NUMBER && _limit <= ChunksSorter::USE_HEAP_SORTER_LIMIT_SZ) {
            chunks_sorter = std::make_unique<ChunksSorterHeapSort>(
                    runtime_state(), &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first,
                    _sort_keys, _offset, _limit);
        } else {
            size_t max_buffered_chunks = ChunksSorterTopn::tunning_buffered_chunks(_limit);
            chunks_sorter = std::make_unique<ChunksSorterTopn>(
                    runtime_state(), &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first,
                    _sort_keys, _offset, _limit, _topn_type, max_buffered_chunks);
        }
    } else {
        chunks_sorter = std::make_unique<vectorized::ChunksSorterFullSort>(runtime_state(),
                                                                           &(_sort_exec_exprs.lhs_ordering_expr_ctxs()),
                                                                           &_is_asc_order, &_is_null_first, _sort_keys);
    }
    auto sort_context = _sort_context_factory->create(driver_sequence);

    sort_context->add_partition_chunks_sorter(chunks_sorter);
    auto ope = std::make_shared<PartitionSortSinkOperator>(
            this, _id, _plan_node_id, driver_sequence, chunks_sorter, _sort_exec_exprs, _order_by_types,
            _materialized_tuple_desc, _parent_node_row_desc, _parent_node_child_row_desc, sort_context.get());
    return ope;
}

void PartitionSortSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_analytic_partition_exprs, state);
    _sort_exec_exprs.close(state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
