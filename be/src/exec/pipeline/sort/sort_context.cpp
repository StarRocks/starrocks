// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/sort/sort_context.h"

namespace starrocks {
namespace pipeline {
SortContextFactory::SortContextFactory(bool is_merging, int64_t limit, const std::vector<bool>& is_asc_order,
                                       const std::vector<bool>& is_null_first)
        : _is_merging(is_merging), _limit(limit), _is_asc_order(is_asc_order), _is_null_first(is_null_first) {}

SortContextPtr SortContextFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    if (_sort_contexts.size() < degree_of_parallelism) {
        _sort_contexts.resize(degree_of_parallelism);
    }

    size_t idx = _is_merging ? 0 : driver_sequence;
    int32_t num_sinkers = _is_merging ? degree_of_parallelism : 1;

    if (_sort_contexts[idx] == nullptr) {
        _sort_contexts[idx] = std::make_shared<SortContext>(_limit, num_sinkers, _is_asc_order, _is_null_first);
    }
    return _sort_contexts[idx];
}
} // namespace pipeline
} // namespace starrocks
