// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "chunks_sorter_full_sort.h"

#include "column/type_traits.h"
#include "exec/vectorized/sorting/merge.h"
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
        : ChunksSorter(state, sort_exprs, is_asc, is_null_first, sort_keys, false) {}

ChunksSorterFullSort::~ChunksSorterFullSort() = default;

Status ChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    size_t target_rows = _total_rows + chunk->num_rows();
    if (target_rows > Column::MAX_CAPACITY_LIMIT) {
        LOG(WARNING) << "Full sort rows exceed limit " << target_rows;
        return Status::InternalError(fmt::format("Full sort rows exceed limit: {}", target_rows));
    }

    // Partial sort
    // TODO: Accumulate to a larger chunk to merge
    {
        SCOPED_TIMER(_sort_timer);
        DataSegment segment(_sort_exprs, chunk);
        Permutation perm;
        sort_and_tie_columns(state->cancelled_ref(), segment.order_by_columns, _sort_order_flag, _null_first_flag,
                             &perm);
        ChunkPtr sorted_chunk = chunk->clone_empty_with_slot(chunk->num_rows());
        append_by_permutation(sorted_chunk.get(), {chunk}, perm);
        _sorted_chunks.push_back(sorted_chunk);
        _total_rows += chunk->num_rows();
    }

    return Status::OK();
}

Status ChunksSorterFullSort::done(RuntimeState* state) {
    RETURN_IF_ERROR(_sort_chunks(state));
    DCHECK_EQ(_next_output_row, 0);
    return Status::OK();
}

void ChunksSorterFullSort::get_next(ChunkPtr* chunk, bool* eos) {
    *eos = pull_chunk(chunk);
}

SortedRuns ChunksSorterFullSort::get_sorted_runs() {
    return _merged_runs;
}

size_t ChunksSorterFullSort::get_output_rows() const {
    return _merged_runs.num_rows();
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
    SCOPED_TIMER(_output_timer);
    if (_merged_runs.num_chunks() == 0) {
        *chunk = nullptr;
        return true;
    }
    *chunk = _merged_runs.front().chunk;
    _merged_runs.pop_front();
    return false;
}

int64_t ChunksSorterFullSort::mem_usage() const {
    return _merged_runs.mem_usage();
}

Status ChunksSorterFullSort::_sort_chunks(RuntimeState* state) {
    SCOPED_TIMER(_sort_timer);

    // Merge sorted segments
    SortDescs sort_desc(_sort_order_flag, _null_first_flag);
    RETURN_IF_ERROR(merge_sorted_chunks(sort_desc, _sort_exprs, _sorted_chunks, &_merged_runs, 0));

    return Status::OK();
}

} // namespace starrocks::vectorized
