// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/sort/sort_context.h"

#include "exec/vectorized/sorting/merge.h"
#include "exec/vectorized/sorting/sorting.h"

namespace starrocks::pipeline {

using vectorized::Permutation;
using vectorized::Columns;
using vectorized::SortedRuns;

ChunkPtr SortContext::pull_chunk() {
    if (!_is_merge_finish) {
        _merge_inputs();
        _is_merge_finish = true;
        return _merged_chunk;
    } else {
        return {};
    }
}

void SortContext::_merge_inputs() {
    int64_t total_rows = _total_rows.load(std::memory_order_relaxed);
    int64_t require_rows = ((_limit < 0) ? total_rows : std::min(_limit, total_rows));

    std::vector<SortedRuns> partial_sorted_runs;
    for (int i = 0; i < _num_partition_sinkers; ++i) {
        auto& partition_sorter = _chunks_sorter_partions[i];
        partial_sorted_runs.push_back(partition_sorter->get_sorted_runs());
    }

    SortedRuns merged_run;
    merge_sorted_chunks(_sort_desc, &_sort_exprs, partial_sorted_runs, &merged_run, require_rows);
    // TODO: avoid assemble
    _merged_chunk = merged_run.assemble();
}

SortContextFactory::SortContextFactory(RuntimeState* state, bool is_merging, int64_t limit, int32_t num_right_sinkers,
                                       const std::vector<ExprContext*>& sort_exprs,
                                       const std::vector<bool>& is_asc_order, const std::vector<bool>& is_null_first)
        : _state(state),
          _is_merging(is_merging),
          _sort_contexts(is_merging ? 1 : num_right_sinkers),
          _limit(limit),
          _num_right_sinkers(num_right_sinkers),
          _sort_exprs(sort_exprs),
          _sort_descs(is_asc_order, is_null_first) {}

SortContextPtr SortContextFactory::create(int32_t idx) {
    size_t actual_idx = _is_merging ? 0 : idx;
    int32_t num_sinkers = _is_merging ? _num_right_sinkers : 1;

    DCHECK_LE(actual_idx, _sort_contexts.size());
    if (!_sort_contexts[actual_idx]) {
        _sort_contexts[actual_idx] =
                std::make_shared<SortContext>(_state, _limit, num_sinkers, _sort_exprs, _sort_descs);
    }
    return _sort_contexts[actual_idx];
}
} // namespace starrocks::pipeline
