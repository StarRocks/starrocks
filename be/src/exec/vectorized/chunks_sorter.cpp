// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/chunks_sorter.h"

#include <type_traits>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

ChunksSorter::ChunksSorter(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>* is_asc,
                           const std::vector<bool>* is_null_first, size_t size_of_chunk_batch)
        : _sort_exprs(sort_exprs), _size_of_chunk_batch(size_of_chunk_batch) {
    DCHECK(_sort_exprs != nullptr);
    DCHECK(is_asc != nullptr);
    DCHECK(is_null_first != nullptr);
    DCHECK_EQ(_sort_exprs->size(), is_asc->size());
    DCHECK_EQ(is_asc->size(), is_null_first->size());

    size_t col_num = is_asc->size();
    _sort_order_flag.resize(col_num);
    _null_first_flag.resize(col_num);
    for (size_t i = 0; i < is_asc->size(); ++i) {
        _sort_order_flag[i] = is_asc->at(i) ? 1 : -1;
        if (is_asc->at(i)) {
            _null_first_flag[i] = is_null_first->at(i) ? -1 : 1;
        } else {
            _null_first_flag[i] = is_null_first->at(i) ? 1 : -1;
        }
    }
}

ChunksSorter::~ChunksSorter() {}

void ChunksSorter::setup_runtime(RuntimeProfile* profile, const std::string& parent_timer) {
    _build_timer = ADD_CHILD_TIMER(profile, "1-BuildingTime", parent_timer);
    _sort_timer = ADD_CHILD_TIMER(profile, "2-SortingTime", parent_timer);
    _merge_timer = ADD_CHILD_TIMER(profile, "3-MergingTime", parent_timer);
    _output_timer = ADD_CHILD_TIMER(profile, "4-OutputTime", parent_timer);
}

Status ChunksSorter::finish(RuntimeState* state) {
    RETURN_IF_ERROR(done(state));
    _is_sink_complete = true;
    return Status::OK();
}

bool ChunksSorter::sink_complete() {
    return _is_sink_complete;
}

vectorized::ChunkPtr ChunksSorter::materialize_chunk_before_sort(vectorized::Chunk* chunk,
                                                                 TupleDescriptor* materialized_tuple_desc,
                                                                 const SortExecExprs& sort_exec_exprs,
                                                                 const std::vector<OrderByType>& order_by_types) {
    vectorized::ChunkPtr materialize_chunk = std::make_shared<vectorized::Chunk>();

    // materialize all sorting columns: replace old columns with evaluated columns
    const size_t row_num = chunk->num_rows();
    const auto& slots_in_row_descriptor = materialized_tuple_desc->slots();
    const auto& slots_in_sort_exprs = sort_exec_exprs.sort_tuple_slot_expr_ctxs();

    DCHECK_EQ(slots_in_row_descriptor.size(), slots_in_sort_exprs.size());

    for (size_t i = 0; i < slots_in_sort_exprs.size(); ++i) {
        ExprContext* expr_ctx = slots_in_sort_exprs[i];
        ColumnPtr col = expr_ctx->evaluate(chunk);
        if (col->is_constant()) {
            if (col->is_nullable()) {
                // Constant null column doesn't have original column data type information,
                // so replace it by a nullable column of original data type filled with all NULLs.
                ColumnPtr new_col = ColumnHelper::create_column(order_by_types[i].type_desc, true);
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
                if (order_by_types[i].is_nullable) {
                    ColumnPtr nullable_column =
                            NullableColumn::create(ColumnPtr(new_col.release()), NullColumn::create(row_num, 0));
                    materialize_chunk->append_column(nullable_column, slots_in_row_descriptor[i]->id());
                } else {
                    materialize_chunk->append_column(ColumnPtr(new_col.release()), slots_in_row_descriptor[i]->id());
                }
            }
        } else {
            // When get a non-null column, but it should be nullable, we wrap it with a NullableColumn.
            if (!col->is_nullable() && order_by_types[i].is_nullable) {
                col = NullableColumn::create(col, NullColumn::create(col->size(), 0));
            }
            materialize_chunk->append_column(col, slots_in_row_descriptor[i]->id());
        }
    }

    return materialize_chunk;
}

} // namespace starrocks::vectorized
