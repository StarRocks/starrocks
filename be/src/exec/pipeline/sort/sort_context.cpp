// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/sort/sort_context.h"

namespace starrocks {
namespace pipeline {
SortContextFactory::SortContextFactory(bool is_merging, int64_t limit, int32_t num_right_sinkers,
                                       const std::vector<bool>& is_asc_order, const std::vector<bool>& is_null_first)
        : _is_merging(is_merging),
          _sort_contexts(is_merging ? 1 : num_right_sinkers),
          _limit(limit),
          _num_right_sinkers(num_right_sinkers),
          _is_asc_order(is_asc_order),
          _is_null_first(is_null_first) {}

SortContextPtr SortContextFactory::create(int i) {
    size_t idx = _is_merging ? 0 : i;
    int32_t num_sinkers = _is_merging ? _num_right_sinkers : 1;

    if (!_sort_contexts[idx]) {
        _sort_contexts[idx] = std::make_shared<SortContext>(_limit, num_sinkers, _is_asc_order, _is_null_first);
    }
    return _sort_contexts[idx];
}
} // namespace pipeline
} // namespace starrocks