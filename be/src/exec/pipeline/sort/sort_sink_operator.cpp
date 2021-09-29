// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/sort/sort_sink_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/mysql_result_writer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status SortSinkOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status SortSinkOperator::close(RuntimeState* state) {
    return Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> SortSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "Shouldn't pull chunk from result sink operator";
}

bool SortSinkOperator::need_input() const {
    return !is_finished();
}

Status SortSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    if (chunk && chunk->num_rows() > 0) {
        vectorized::ChunkPtr materialize_chunk = _materialize_chunk_before_sort(chunk.get());
        _chunks_sorter->update(state, materialize_chunk);
    }

    return Status::OK();
}

vectorized::ChunkPtr SortSinkOperator::_materialize_chunk_before_sort(vectorized::Chunk* chunk) {
    vectorized::ChunkPtr materialize_chunk = std::make_shared<vectorized::Chunk>();

    // materialize all sorting columns: replace old columns with evaluated columns
    const size_t row_num = chunk->num_rows();
    const auto& slots_in_row_descriptor = _materialized_tuple_desc->slots();
    const auto& slots_in_sort_exprs = _sort_exec_exprs.sort_tuple_slot_expr_ctxs();

    DCHECK_EQ(slots_in_row_descriptor.size(), slots_in_sort_exprs.size());

    for (size_t i = 0; i < slots_in_sort_exprs.size(); ++i) {
        ExprContext* expr_ctx = slots_in_sort_exprs[i];
        ColumnPtr col = expr_ctx->evaluate(chunk);
        if (col->is_constant()) {
            if (col->is_nullable()) {
                // Constant null column doesn't have original column data type information,
                // so replace it by a nullable column of original data type filled with all NULLs.
                ColumnPtr new_col = ColumnHelper::create_column(_order_by_types[i].type_desc, true);
                new_col->append_nulls(row_num);
                materialize_chunk->append_column(new_col, slots_in_row_descriptor[i]->id());
            } else {
                // Case 1: an expression may generate a constant column which will be reused by
                // another call of evaluate(). We clone its data column to resize it as same as
                // the size of the chunk, so that Chunk::num_rows() can return the right number
                // if this ConstColumn is the first column of the chunk.
                // Case 2: an expression may generate a constant column for one Chunk, but a
                // non-constant one for another Chunk, we replace them all by non-constant columns.
                auto* const_col = down_cast<ConstColumn*>(col.get());
                const auto& data_col = const_col->data_column();
                auto new_col = data_col->clone_empty();
                new_col->append(*data_col, 0, 1);
                new_col->assign(row_num, 0);
                if (_order_by_types[i].is_nullable) {
                    ColumnPtr nullable_column =
                            NullableColumn::create(ColumnPtr(new_col.release()), NullColumn::create(row_num, 0));
                    materialize_chunk->append_column(nullable_column, slots_in_row_descriptor[i]->id());
                } else {
                    materialize_chunk->append_column(ColumnPtr(new_col.release()), slots_in_row_descriptor[i]->id());
                }
            }
        } else {
            // When get a non-null column, but it should be nullable, we wrap it with a NullableColumn.
            if (!col->is_nullable() && _order_by_types[i].is_nullable) {
                col = NullableColumn::create(col, NullColumn::create(col->size(), 0));
            }
            materialize_chunk->append_column(col, slots_in_row_descriptor[i]->id());
        }
    }

    return materialize_chunk;
}

void SortSinkOperator::finish(RuntimeState* state) {
    _chunks_sorter->finish(state);
    _is_finished = true;
}

Status SortSinkOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, _parent_node_row_desc, _parent_node_child_row_desc, mem_tracker));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));
    return Status::OK();
}

void SortSinkOperatorFactory::close(RuntimeState* state) {
    _sort_exec_exprs.close(state);
}

} // namespace starrocks::pipeline
