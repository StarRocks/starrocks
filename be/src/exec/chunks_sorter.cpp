// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/chunks_sorter.h"

#include <utility>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exec/sorting/sort_permute.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks {

void DataSegment::init(const std::vector<ExprContext*>* sort_exprs, const ChunkPtr& cnk) {
    chunk = cnk;
    order_by_columns.reserve(sort_exprs->size());
    for (ExprContext* expr_ctx : (*sort_exprs)) {
        order_by_columns.push_back(EVALUATE_NULL_IF_ERROR(expr_ctx, expr_ctx->root(), chunk.get()));
    }
}

ChunksSorter::ChunksSorter(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                           const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first,
                           std::string sort_keys, const bool is_topn)
        : _state(state),
          _sort_exprs(sort_exprs),
          _sort_desc(*is_asc, *is_null_first),
          _sort_keys(std::move(sort_keys)),
          _is_topn(is_topn) {
    DCHECK(_sort_exprs != nullptr);
    DCHECK(is_asc != nullptr);
    DCHECK(is_null_first != nullptr);
    DCHECK_EQ(_sort_exprs->size(), is_asc->size());
    DCHECK_EQ(is_asc->size(), is_null_first->size());
}

ChunksSorter::~ChunksSorter() = default;

void ChunksSorter::setup_runtime(RuntimeState* state, RuntimeProfile* profile, MemTracker* parent_mem_tracker) {
    _build_timer = ADD_TIMER(profile, "BuildingTime");
    _sort_timer = ADD_TIMER(profile, "SortingTime");
    _merge_timer = ADD_TIMER(profile, "MergingTime");
    _output_timer = ADD_TIMER(profile, "OutputTime");
    profile->add_info_string("SortKeys", _sort_keys);
    profile->add_info_string("SortType", _is_topn ? "TopN" : "All");
}

StatusOr<ChunkPtr> ChunksSorter::materialize_chunk_before_sort(Chunk* chunk, TupleDescriptor* materialized_tuple_desc,
                                                               const SortExecExprs& sort_exec_exprs,
                                                               const std::vector<OrderByType>& order_by_types) {
    ChunkPtr materialize_chunk = std::make_shared<Chunk>();

    // materialize all sorting columns: replace old columns with evaluated columns
    const size_t row_num = chunk->num_rows();
    const auto& slots_in_row_descriptor = materialized_tuple_desc->slots();
    const auto& slots_in_sort_exprs = sort_exec_exprs.sort_tuple_slot_expr_ctxs();

    DCHECK_EQ(slots_in_row_descriptor.size(), slots_in_sort_exprs.size());

    for (size_t i = 0; i < slots_in_sort_exprs.size(); ++i) {
        ExprContext* expr_ctx = slots_in_sort_exprs[i];
        ColumnPtr col = EVALUATE_NULL_IF_ERROR(expr_ctx, expr_ctx->root(), chunk);
        if (col->is_constant()) {
            if (col->is_nullable()) {
                // Constant null column doesn't have original column data type information,
                // so replace it by a nullable column of original data type filled with all NULLs.
                MutableColumnPtr new_col = ColumnHelper::create_column(order_by_types[i].type_desc, true);
                new_col->append_nulls(row_num);
                materialize_chunk->append_column(std::move(new_col), slots_in_row_descriptor[i]->id());
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
                            NullableColumn::create(ColumnPtr(std::move(new_col)), NullColumn::create(row_num, 0));
                    materialize_chunk->append_column(nullable_column, slots_in_row_descriptor[i]->id());
                } else {
                    materialize_chunk->append_column(ColumnPtr(std::move(new_col)), slots_in_row_descriptor[i]->id());
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

Status ChunksSorter::done(RuntimeState* state) {
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(do_done(state)));
    return Status::OK();
}

} // namespace starrocks
