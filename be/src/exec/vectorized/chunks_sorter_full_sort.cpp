// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "chunks_sorter_full_sort.h"

#include "exec/vectorized//sorting/sort_helper.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

// SortHelper functions only work for full sort.

ChunksSorterFullSort::ChunksSorterFullSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                           const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first,
                                           size_t size_of_chunk_batch)
        : ChunksSorter(state, sort_exprs, is_asc, is_null_first, size_of_chunk_batch) {
    _selective_values.resize(_state->chunk_size());
}

ChunksSorterFullSort::~ChunksSorterFullSort() = default;

Status ChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    if (UNLIKELY(_big_chunk == nullptr)) {
        _big_chunk = chunk->clone_empty();
    }

    if (_big_chunk->num_rows() + chunk->num_rows() > std::numeric_limits<uint32_t>::max()) {
        LOG(WARNING) << "full sort row is " << _big_chunk->num_rows() + chunk->num_rows();
        return Status::InternalError("Full sort in single query instance only support at most 4294967295 rows");
    }

    _big_chunk->append(*chunk);

    DCHECK(!_big_chunk->has_const_column());
    return Status::OK();
}

Status ChunksSorterFullSort::done(RuntimeState* state) {
    if (_big_chunk != nullptr && _big_chunk->num_rows() > 0) {
        RETURN_IF_ERROR(_sort_chunks(state));
    }

    DCHECK_EQ(_next_output_row, 0);
    return Status::OK();
}

void ChunksSorterFullSort::get_next(ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_output_timer);
    if (_next_output_row >= _sorted_permutation.size()) {
        *chunk = nullptr;
        *eos = true;
        return;
    }
    *eos = false;
    size_t count = std::min(size_t(_state->chunk_size()), _sorted_permutation.size() - _next_output_row);
    chunk->reset(_sorted_segment->chunk->clone_empty(count).release());
    _append_rows_to_chunk(chunk->get(), _sorted_segment->chunk.get(), _sorted_permutation, _next_output_row, count);
    _next_output_row += count;
}

DataSegment* ChunksSorterFullSort::get_result_data_segment() {
    return _sorted_segment.get();
}

uint64_t ChunksSorterFullSort::get_partition_rows() const {
    return _sorted_permutation.size();
}

// Is used to index sorted datas.
Permutation* ChunksSorterFullSort::get_permutation() const {
    return &_sorted_permutation;
}

/*
 * _next_output_row index the next row we need to get,  
 * _sorted_permutation means all the result datas. In this case, 
 * _sorted_permutation use as an index, 
 * The actual data is _sorted_segment->chunk, 
 * so we use _next_output_row and _sorted_permutation to get datas from _sorted_segment->chunk, 
 * and copy it in chunk as output.
 */
bool ChunksSorterFullSort::pull_chunk(ChunkPtr* chunk) {
    // _next_output_row used to record next row to get,
    // This condition is used to determine whether all data has been retrieved.
    if (_next_output_row >= _sorted_permutation.size()) {
        *chunk = nullptr;
        return true;
    }
    size_t count = std::min(size_t(_state->chunk_size()), _sorted_permutation.size() - _next_output_row);
    chunk->reset(_sorted_segment->chunk->clone_empty(count).release());
    _append_rows_to_chunk(chunk->get(), _sorted_segment->chunk.get(), _sorted_permutation, _next_output_row, count);
    _next_output_row += count;

    return _next_output_row >= _sorted_permutation.size();
}

int64_t ChunksSorterFullSort::mem_usage() const {
    int64_t usage = 0;
    if (_big_chunk != nullptr) {
        usage += _big_chunk->memory_usage();
    }
    if (_sorted_segment != nullptr) {
        usage += _sorted_segment->mem_usage();
    }
    usage += _sorted_permutation.capacity() * sizeof(Permutation);
    usage += _selective_values.capacity() * sizeof(uint32_t);
    return usage;
}

Status ChunksSorterFullSort::_sort_chunks(RuntimeState* state) {
    // Step1: construct permutation
    RETURN_IF_ERROR(_build_sorting_data(state));

    // Step2: sort by columns or row
    // For no more than three order-by columns, sorting by columns can benefit from reducing
    // the cost of calling virtual functions of Column::compare_at.
    CompareStrategy strategy = Default;
    if (_compare_strategy != Default) {
        strategy = _compare_strategy;
    } else {
        if (_get_number_of_order_by_columns() <= 3) {
            strategy = ColumnWise;
        } else {
            strategy = RowWise;
        }
    }
    if (strategy == ColumnWise) {
        RETURN_IF_ERROR(_sort_by_columns(state));
    } else {
        RETURN_IF_ERROR(_sort_by_row_cmp(state));
    }
    return Status::OK();
}

Status ChunksSorterFullSort::_build_sorting_data(RuntimeState* state) {
    SCOPED_TIMER(_build_timer);
    size_t row_count = _big_chunk->num_rows();

    _sorted_segment = std::make_unique<DataSegment>(_sort_exprs, ChunkPtr(_big_chunk.release()));

    _sorted_permutation.resize(row_count);
    for (uint32_t i = 0; i < row_count; ++i) {
        _sorted_permutation[i] = {0, i, i};
    }

    return Status::OK();
}

// Sort in row style with simplified Permutation struct for the seek of a better cache.
Status ChunksSorterFullSort::_sort_by_row_cmp(RuntimeState* state) {
    SCOPED_TIMER(_sort_timer);

    if (_get_number_of_order_by_columns() < 1) {
        return Status::OK();
    }

    // In this case, PermutationItem::chunk_index is constantly 0,
    // and PermutationItem::index_in_chunk is always equal to PermutationItem::permutation_index,
    // which is the sequence index of the element in the array.
    // This simplified index array can help the sort routine to get a better performance.
    const size_t elem_number = _sorted_permutation.size();
    std::vector<size_t> indices(elem_number);
    for (size_t i = 0; i < elem_number; ++i) {
        indices[i] = i;
    }

    const DataSegment& data_segment = *_sorted_segment;
    const std::vector<int>& sort_order_flag = _sort_order_flag;
    const std::vector<int>& null_first_flag = _null_first_flag;

    auto cmp_fn = [&data_segment, &sort_order_flag, &null_first_flag](const size_t& l, const size_t& r) {
        int c = data_segment.compare_at(l, data_segment, r, sort_order_flag, null_first_flag);
        if (c == 0) {
            return l < r;
        } else {
            return c < 0;
        }
    };

    pdqsort(state->cancelled_ref(), indices.begin(), indices.end(), cmp_fn);
    RETURN_IF_CANCELLED(state);

    // Set the permutation array to sorted indices.
    for (size_t i = 0; i < elem_number; ++i) {
        _sorted_permutation[i].index_in_chunk = _sorted_permutation[i].permutation_index = indices[i];
    }
    return Status::OK();
}

// Sort in column style to avoid calling virtual methods of Column.
Status ChunksSorterFullSort::_sort_by_columns(RuntimeState* state) {
    SCOPED_TIMER(_sort_timer);

    int num_columns = _get_number_of_order_by_columns();
    if (num_columns < 1) {
        return Status::OK();
    }

    std::vector<SortHelper::SingleColumnSortDesc> sort_descs;
    for (int col_index = num_columns - 1; col_index >= 0; --col_index) {
        bool is_asc_order = (_sort_order_flag[col_index] == 1);
        bool is_null_first;
        if (is_asc_order) {
            is_null_first = (_null_first_flag[col_index] == -1);
        } else {
            is_null_first = (_null_first_flag[col_index] == 1);
        }

        ExprContext* expr_ctx = (*_sort_exprs)[col_index];
        PrimitiveType sort_type = expr_ctx->root()->type().type;

        SortHelper::SingleColumnSortDesc desc;
        desc.is_asc = is_asc_order;
        desc.null_first = is_null_first;
        desc.sort_type = sort_type;

        sort_descs.emplace_back(desc);
    }

    return SortHelper::sort_multi_column(state, _sorted_segment->order_by_columns, sort_descs, _sorted_permutation);
}

void ChunksSorterFullSort::_append_rows_to_chunk(Chunk* dest, Chunk* src, const Permutation& permutation, size_t offset,
                                                 size_t count) {
    for (size_t i = offset; i < offset + count; ++i) {
        _selective_values[i - offset] = permutation[i].index_in_chunk;
    }
    dest->append_selective(*src, _selective_values.data(), 0, count);

    DCHECK(!dest->has_const_column());
}

} // namespace starrocks::vectorized
