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

#include "exprs/chunk_predicate_evaluator.h"

#include <algorithm>

#include "base/simd/simd.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/compiler_util.h"
#include "common/logging.h"
#include "exprs/expr_context.h"
#include "runtime/current_thread.h"

namespace starrocks {

namespace {

Status eager_prune_eval_conjuncts(const std::vector<ExprContext*>& ctxs, Chunk* chunk) {
    Filter filter(chunk->num_rows(), 1);
    Filter* raw_filter = &filter;

    // prune chunk when pruned size is large enough
    // these constants are just came up without any specific reason.
    // and plus there is no strong evidence that this strategy has better performance.
    // pruned chunk can be saved from following conjuncts evaluation, that's benefit.
    // but calling `filter` there is memory copy, that's cost. So this is tradeoff.
    // so we don't expect pruned chunk size is too small. and if prune_ratio > 0.5
    // there is at most one prune process. according to my observation to running tpch 100g
    // in many cases, chunk is sparse enough.
    const float prune_ratio = 0.8;
    const int prune_min_size = 1024;

    int prune_threshold = std::max(int(chunk->num_rows() * prune_ratio), prune_min_size);
    int zero_count = 0;

    for (auto* ctx : ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(chunk))
        size_t true_count = ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            chunk->set_num_rows(0);
            return Status::OK();
        } else {
            ColumnHelper::merge_two_filters(column, raw_filter, nullptr);
            zero_count = SIMD::count_zero(*raw_filter);
            if (zero_count > prune_threshold) {
                int rows = chunk->filter(*raw_filter, true);
                if (rows == 0) {
                    // When all rows in chunk is filtered, direct return
                    // No need to execute the following predicate
                    return Status::OK();
                }
                filter.assign(rows, 1);
                zero_count = 0;
            }
        }
    }
    if (zero_count == 0) {
        return Status::OK();
    }
    chunk->filter(*raw_filter, true);
    return Status::OK();
}

} // namespace

Status ChunkPredicateEvaluator::eval_conjuncts(const std::vector<ExprContext*>& ctxs, Chunk* chunk,
                                               FilterPtr* filter_ptr, bool apply_filter) {
    // No need to do expression if none rows
    DCHECK(chunk != nullptr);
    if (chunk->num_rows() == 0) {
        return Status::OK();
    }

    // if we don't need filter, then we can prune chunk during eval conjuncts.
    // when doing prune, we expect all columns are in conjuncts, otherwise
    // there will be extra memcpy of columns/slots which are not children of any conjunct.
    // ideally, we can collects slots in conjuncts, and check the overlap with chunk->columns
    // if overlap ratio is high enough, it's good to do prune.
    // but here for simplicity, we just check columns numbers absolute value.
    // TO BE NOTED, that there is no storng evidence that this has better performance.
    // It's just by intuition.
    TRY_CATCH_ALLOC_SCOPE_START()
    const int eager_prune_max_column_number = 5;
    if (filter_ptr == nullptr && chunk->num_columns() <= eager_prune_max_column_number) {
        return eager_prune_eval_conjuncts(ctxs, chunk);
    }

    if (!apply_filter) {
        DCHECK(filter_ptr) << "Must provide a filter if not apply it directly";
    }
    FilterPtr filter(new Filter(chunk->num_rows(), 1));
    if (filter_ptr != nullptr) {
        *filter_ptr = filter;
    }
    Filter* raw_filter = filter.get();

    for (auto* ctx : ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(chunk))
        size_t true_count = ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            // all not hit, return
            if (apply_filter) {
                chunk->set_num_rows(0);
            } else {
                filter->assign(filter->size(), 0);
            }
            return Status::OK();
        } else {
            bool all_zero = false;
            ColumnHelper::merge_two_filters(column, raw_filter, &all_zero);
            if (all_zero) {
                if (apply_filter) {
                    chunk->set_num_rows(0);
                } else {
                    filter->assign(filter->size(), 0);
                }
                return Status::OK();
            }
        }
    }

    if (apply_filter) {
        chunk->filter(*raw_filter);
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

StatusOr<size_t> ChunkPredicateEvaluator::eval_conjuncts_into_filter(const std::vector<ExprContext*>& ctxs,
                                                                     Chunk* chunk, Filter* filter) {
    // No need to do expression if none rows
    DCHECK(chunk != nullptr);
    if (chunk->num_rows() == 0) {
        return 0;
    }
    for (auto* ctx : ctxs) {
        ASSIGN_OR_RETURN(ColumnPtr column, ctx->evaluate(chunk, filter->data()))
        size_t true_count = ColumnHelper::count_true_with_notnull(column);

        if (true_count == column->size()) {
            // all hit, skip
            continue;
        } else if (0 == true_count) {
            return 0;
        } else {
            bool all_zero = false;
            ColumnHelper::merge_two_filters(column, filter, &all_zero);
            if (all_zero) {
                return 0;
            }
        }
    }

    size_t true_count = SIMD::count_nonzero(*filter);
    return true_count;
}

void ChunkPredicateEvaluator::eval_filter_null_values(Chunk* chunk,
                                                      const std::vector<SlotId>& filter_null_value_columns) {
    if (filter_null_value_columns.size() == 0) return;
    size_t before_size = chunk->num_rows();
    if (before_size == 0) return;

    // lazy allocation.
    Buffer<uint8_t> selection(0);

    for (SlotId slot_id : filter_null_value_columns) {
        const ColumnPtr& c = chunk->get_column_by_slot_id(slot_id);
        if (!c->is_nullable()) continue;
        if (c->only_null()) {
            chunk->reset();
            return;
        }
        const NullableColumn* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(c);
        if (!nullable_column->has_null()) continue;
        if (selection.size() == 0) {
            selection.assign(before_size, 1);
        }
        // how many data() should we call? I really don't know.
        // let compiler tells me.
        // let compiler does vectorization.
        // let compiler does everything.
        // let's pray for compiler,
        // till the end of the world.
        const uint8_t* nulls = nullable_column->null_column()->raw_data();
        uint8_t* sel = selection.data();
        for (size_t i = 0; i < before_size; i++) {
            sel[i] &= !nulls[i];
        }
    }
    if (selection.size() == 0) return;

    size_t after_size = SIMD::count_nonzero(selection);
    // Those rows will be filtered out anyway, better to be filtered out here.
    if (after_size != before_size) {
        VLOG_FILE << "filter null values. before_size = " << before_size << ", after_size = " << after_size;
        chunk->filter(selection, true);
    }
}

} // namespace starrocks
