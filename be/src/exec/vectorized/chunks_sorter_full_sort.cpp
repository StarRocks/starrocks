// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "chunks_sorter_full_sort.h"

#include "column/type_traits.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/primitive_type_infra.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

ChunksSorterFullSort::ChunksSorterFullSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                           const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first,
                                           const std::string& sort_keys)
        : ChunksSorter(state, sort_exprs, is_asc, is_null_first, sort_keys, false) {
    _selective_values.resize(_state->chunk_size());
}

ChunksSorterFullSort::~ChunksSorterFullSort() = default;

Status ChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    if (UNLIKELY(_big_chunk == nullptr)) {
        _big_chunk = chunk->clone_empty();
    }

    size_t target_rows = _big_chunk->num_rows() + chunk->num_rows();
    if (target_rows > Column::MAX_CAPACITY_LIMIT) {
        LOG(WARNING) << "Full sort rows exceed limit " << target_rows;
        return Status::InternalError(fmt::format("Full sort rows exceed limit: {}", target_rows));
    }

    _big_chunk->append(*chunk);

    if (_big_chunk->reach_capacity_limit()) {
        LOG(WARNING) << "Full sort encounter big chunk overflow issue";
        return Status::InternalError(fmt::format("Full sort encounter big chunk overflow issue"));
    }

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
    return _sort_by_column_inc(state);
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

// Sort in column-wise and incremental style
Status ChunksSorterFullSort::_sort_by_column_inc(RuntimeState* state) {
    SCOPED_TIMER(_sort_timer);

    return sort_and_tie_columns(state->cancelled_ref(), _sorted_segment->order_by_columns, _sort_order_flag,
                                _null_first_flag, &_sorted_permutation);
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
