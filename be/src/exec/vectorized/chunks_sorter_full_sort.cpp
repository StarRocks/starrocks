// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "chunks_sorter_full_sort.h"

#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

ChunksSorterFullSort::ChunksSorterFullSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                           const std::vector<bool>* is_asc_order,
                                           const std::vector<bool>* is_null_first, const std::string& sort_keys)
        : ChunksSorter(state, sort_exprs, is_asc_order, is_null_first, sort_keys, false) {}

ChunksSorterFullSort::~ChunksSorterFullSort() = default;

Status ChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    _merge_unsorted(state, chunk);
    _partial_sort(state, false);

    return Status::OK();
}

// Accumulate unsorted input chunks into a larger chunk
Status ChunksSorterFullSort::_merge_unsorted(RuntimeState* state, const ChunkPtr& chunk) {
    SCOPED_TIMER(_build_timer);

    if (_unsorted_chunk == nullptr) {
        // TODO: optimize the copy
        _unsorted_chunk.reset(chunk->clone_unique().release());
    } else {
        _unsorted_chunk->append(*chunk);
    }

    return Status::OK();
}

// Sort the large chunk
Status ChunksSorterFullSort::_partial_sort(RuntimeState* state, bool done) {
    if (!_unsorted_chunk) {
        return Status::OK();
    }
    bool reach_limit = _unsorted_chunk->num_rows() >= kMaxBufferedChunkSize || _unsorted_chunk->reach_capacity_limit();
    if (done || reach_limit) {
        SCOPED_TIMER(_sort_timer);

        // Check column overflow problem
        // TODO upgrade to large binary column if overflow
        if (_unsorted_chunk->reach_capacity_limit()) {
            LOG(WARNING) << fmt::format("Sorter encounter big chunk overflow with {} rows and {} bytes",
                                        _unsorted_chunk->num_rows(), _unsorted_chunk->bytes_usage());
            return Status::InternalError("Sorter encounter big chunk overflow");
        }

        DataSegment segment(_sort_exprs, _unsorted_chunk);
        _sort_permutation.resize(0);
        RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), segment.order_by_columns, _sort_order_flag,
                                             _null_first_flag, &_sort_permutation));
        ChunkPtr sorted_chunk = _unsorted_chunk->clone_empty_with_slot(_unsorted_chunk->num_rows());
        append_by_permutation(sorted_chunk.get(), {_unsorted_chunk}, _sort_permutation);

        _sorted_chunks.push_back(sorted_chunk);
        _total_rows += _unsorted_chunk->num_rows();
        _unsorted_chunk.reset();
    }

    return Status::OK();
}

Status ChunksSorterFullSort::_merge_sorted(RuntimeState* state) {
    SCOPED_TIMER(_merge_timer);

    SortDescs sort_desc(_sort_order_flag, _null_first_flag);
    RETURN_IF_ERROR(merge_sorted_chunks(sort_desc, _sort_exprs, _sorted_chunks, &_merged_runs, 0));

    return Status::OK();
}

Status ChunksSorterFullSort::done(RuntimeState* state) {
    RETURN_IF_ERROR(_partial_sort(state, true));
    RETURN_IF_ERROR(_merge_sorted(state));
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

bool ChunksSorterFullSort::pull_chunk(ChunkPtr* chunk) {
    SCOPED_TIMER(_output_timer);
    if (_merged_runs.num_chunks() == 0) {
        *chunk = nullptr;
        return true;
    }
    size_t chunk_size = _state->chunk_size();
    SortedRun& run = _merged_runs.front();
    *chunk = run.steal_chunk(chunk_size);
    if (run.empty()) {
        _merged_runs.pop_front();
    }
    return false;
}

int64_t ChunksSorterFullSort::mem_usage() const {
    return _merged_runs.mem_usage();
}

} // namespace starrocks::vectorized
