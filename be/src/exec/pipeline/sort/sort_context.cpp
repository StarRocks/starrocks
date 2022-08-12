// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/sort/sort_context.h"

#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sorting.h"

namespace starrocks::pipeline {

using vectorized::Permutation;
using vectorized::Columns;
using vectorized::SortedRun;
using vectorized::SortedRuns;

void SortContext::close(RuntimeState* state) {
    _chunks_sorter_partions.clear();
    _merged_runs.clear();
}

StatusOr<ChunkPtr> SortContext::pull_chunk() {
    if (!_is_merge_finish) {
        _merge_inputs();
        _is_merge_finish = true;
    }
    if (_merged_runs.num_chunks() == 0) {
        return nullptr;
    }
    size_t required_rows = _state->chunk_size();
    required_rows = std::min<size_t>(required_rows, _total_rows);
    if (_limit > 0 && _topn_type == TTopNType::ROW_NUMBER) {
        required_rows = std::min<size_t>(required_rows, _limit);
    }

    SortedRun& run = _merged_runs.front();
    size_t rows_before = run.num_rows();
    ChunkPtr res = run.steal_chunk(required_rows, _offset);
    size_t rows_after = run.num_rows();
    size_t fetched_rows = res == nullptr ? 0 : res->num_rows();
    size_t skipped_rows = rows_before - rows_after - fetched_rows;
    _offset -= skipped_rows;
    if (res != nullptr) {
        RETURN_IF_ERROR(res->downgrade());
    }

    if (run.empty()) {
        _merged_runs.pop_front();
    }
    return res;
}

Status SortContext::_merge_inputs() {
    int64_t total_rows = _total_rows.load(std::memory_order_relaxed);

    // Keep all the data if topn type is RANK or DENSE_RANK
    int64_t require_rows = total_rows;
    if (_topn_type == TTopNType::ROW_NUMBER) {
        require_rows = ((_limit < 0) ? total_rows : std::min(_limit + _offset, total_rows));
    }

    std::vector<SortedRuns> partial_sorted_runs;
    for (int i = 0; i < _num_partition_sinkers; ++i) {
        auto& partition_sorter = _chunks_sorter_partions[i];
        partial_sorted_runs.push_back(partition_sorter->get_sorted_runs());
    }

    return merge_sorted_chunks(_sort_desc, &_sort_exprs, partial_sorted_runs, &_merged_runs, require_rows);
}

SortContextFactory::SortContextFactory(RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
                                       int64_t offset, int64_t limit, int32_t num_right_sinkers,
                                       const std::vector<ExprContext*>& sort_exprs,
                                       const std::vector<bool>& is_asc_order, const std::vector<bool>& is_null_first)
        : _state(state),
          _topn_type(topn_type),
          _is_merging(is_merging),
          _sort_contexts(is_merging ? 1 : num_right_sinkers),
          _offset(offset),
          _limit(limit),
          _num_right_sinkers(num_right_sinkers),
          _sort_exprs(sort_exprs),
          _sort_descs(is_asc_order, is_null_first) {}

SortContextPtr SortContextFactory::create(int32_t idx) {
    size_t actual_idx = _is_merging ? 0 : idx;
    int32_t num_sinkers = _is_merging ? _num_right_sinkers : 1;

    DCHECK_LE(actual_idx, _sort_contexts.size());
    if (!_sort_contexts[actual_idx]) {
        _sort_contexts[actual_idx] = std::make_shared<SortContext>(_state, _topn_type, _offset, _limit, num_sinkers,
                                                                   _sort_exprs, _sort_descs);
    }
    return _sort_contexts[actual_idx];
}
} // namespace starrocks::pipeline
