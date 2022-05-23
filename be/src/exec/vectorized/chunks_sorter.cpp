// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/chunks_sorter.h"

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

static void get_compare_results_colwise(size_t number_of_rows_to_sort, Columns& order_by_columns,
                                        std::vector<CompareVector>& compare_results_array,
                                        std::vector<DataSegment>& data_segments,
                                        const std::vector<int>& sort_order_flags,
                                        const std::vector<int>& null_first_flags) {
    size_t dats_segment_size = data_segments.size();

    for (size_t i = 0; i < dats_segment_size; ++i) {
        size_t rows = data_segments[i].chunk->num_rows();
        compare_results_array[i].resize(rows, 0);
    }

    size_t order_by_column_size = order_by_columns.size();

    for (size_t i = 0; i < dats_segment_size; i++) {
        std::vector<Datum> rhs_values;
        auto& segment = data_segments[i];
        for (size_t col_idx = 0; col_idx < order_by_column_size; col_idx++) {
            rhs_values.push_back(order_by_columns[col_idx]->get(number_of_rows_to_sort));
        }
        compare_columns(segment.order_by_columns, compare_results_array[i], rhs_values, sort_order_flags,
                        null_first_flags);
    }
}

Status DataSegment::get_filter_array(std::vector<DataSegment>& data_segments, size_t number_of_rows_to_sort,
                                     std::vector<std::vector<uint8_t>>& filter_array,
                                     const std::vector<int>& sort_order_flags, const std::vector<int>& null_first_flags,
                                     uint32_t& least_num, uint32_t& middle_num) {
    size_t dats_segment_size = data_segments.size();
    std::vector<CompareVector> compare_results_array(dats_segment_size);

    // first compare with last row of this chunk.
    {
        get_compare_results_colwise(number_of_rows_to_sort - 1, order_by_columns, compare_results_array, data_segments,
                                    sort_order_flags, null_first_flags);
    }

    // but we only have one compare.
    // compare with first row of this DataSegment,
    // then we set BEFORE_LAST_RESULT and IN_LAST_RESULT at filter_array.
    if (number_of_rows_to_sort == 1) {
        least_num = 0, middle_num = 0;
        filter_array.resize(dats_segment_size);
        for (size_t i = 0; i < dats_segment_size; ++i) {
            size_t rows = data_segments[i].chunk->num_rows();
            filter_array[i].resize(rows);

            for (size_t j = 0; j < rows; ++j) {
                if (compare_results_array[i][j] < 0) {
                    filter_array[i][j] = DataSegment::BEFORE_LAST_RESULT;
                    ++least_num;
                } else {
                    filter_array[i][j] = DataSegment::IN_LAST_RESULT;
                    ++middle_num;
                }
            }
        }
    } else {
        std::vector<size_t> first_size_array;
        first_size_array.resize(dats_segment_size);

        middle_num = 0;
        filter_array.resize(dats_segment_size);
        for (size_t i = 0; i < dats_segment_size; ++i) {
            DataSegment& segment = data_segments[i];
            size_t rows = segment.chunk->num_rows();
            filter_array[i].resize(rows);

            size_t local_first_size = middle_num;
            for (size_t j = 0; j < rows; ++j) {
                if (compare_results_array[i][j] < 0) {
                    filter_array[i][j] = DataSegment::IN_LAST_RESULT;
                    ++middle_num;
                } else if (compare_results_array[i][j] == 0) {
                    filter_array[i][j] = DataSegment::IN_LAST_RESULT;
                    ++middle_num;
                }
            }

            // obtain number of rows for second compare.
            first_size_array[i] = middle_num - local_first_size;
        }

        // second compare with first row of this chunk, use rows from first compare.
        {
            for (size_t i = 0; i < dats_segment_size; i++) {
                for (auto& cmp : compare_results_array[i]) {
                    if (cmp < 0) {
                        cmp = 0;
                    }
                }
            }
            get_compare_results_colwise(0, order_by_columns, compare_results_array, data_segments, sort_order_flags,
                                        null_first_flags);
        }

        least_num = 0;
        for (size_t i = 0; i < dats_segment_size; ++i) {
            DataSegment& segment = data_segments[i];
            size_t rows = segment.chunk->num_rows();

            for (size_t j = 0; j < rows; ++j) {
                if (compare_results_array[i][j] < 0) {
                    filter_array[i][j] = DataSegment::BEFORE_LAST_RESULT;
                    ++least_num;
                }
            }
        }
        middle_num -= least_num;
    }

    return Status::OK();
}

ChunksSorter::ChunksSorter(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                           const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first,
                           const std::string& sort_keys, const bool is_topn)
        : _state(state), _sort_exprs(sort_exprs), _sort_keys(sort_keys), _is_topn(is_topn) {
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

void ChunksSorter::setup_runtime(RuntimeProfile* profile) {
    _build_timer = ADD_TIMER(profile, "BuildingTime");
    _sort_timer = ADD_TIMER(profile, "SortingTime");
    _merge_timer = ADD_TIMER(profile, "MergingTime");
    _output_timer = ADD_TIMER(profile, "OutputTime");
    profile->add_info_string("SortKeys", _sort_keys);
    profile->add_info_string("SortType", _is_topn ? "TopN" : "All");
}

Status ChunksSorter::finish(RuntimeState* state) {
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(done(state)));
    _is_sink_complete = true;
    return Status::OK();
}

bool ChunksSorter::sink_complete() {
    return _is_sink_complete;
}

StatusOr<vectorized::ChunkPtr> ChunksSorter::materialize_chunk_before_sort(
        vectorized::Chunk* chunk, TupleDescriptor* materialized_tuple_desc, const SortExecExprs& sort_exec_exprs,
        const std::vector<OrderByType>& order_by_types) {
    vectorized::ChunkPtr materialize_chunk = std::make_shared<vectorized::Chunk>();

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
