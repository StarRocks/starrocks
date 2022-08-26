// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/sort/sort_context.h"

namespace starrocks {
namespace pipeline {

SortContextFactory::SortContextFactory(RuntimeState* state, bool is_merging, int64_t offset, int64_t limit,
                                       int32_t num_right_sinkers, const std::vector<bool>& is_asc_order,
                                       const std::vector<bool>& is_null_first)
        : _state(state),
          _is_merging(is_merging),
          _sort_contexts(is_merging ? 1 : num_right_sinkers),
          _offset(offset),
          _limit(limit),
          _num_right_sinkers(num_right_sinkers),
          _is_asc_order(is_asc_order),
          _is_null_first(is_null_first) {}

SortContextPtr SortContextFactory::create(int32_t idx) {
    size_t actual_idx = _is_merging ? 0 : idx;
    int32_t num_sinkers = _is_merging ? _num_right_sinkers : 1;

    DCHECK_LE(actual_idx, _sort_contexts.size());
    if (!_sort_contexts[actual_idx]) {
        _sort_contexts[actual_idx] =
                std::make_shared<SortContext>(_state, _offset, _limit, num_sinkers, _is_asc_order, _is_null_first);
    }
    return _sort_contexts[actual_idx];
}
} // namespace pipeline
} // namespace starrocks
