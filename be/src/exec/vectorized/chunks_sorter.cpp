// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/chunks_sorter.h"

#include <type_traits>

#include "column/type_traits.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

ChunksSorter::ChunksSorter(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>* is_asc,
                           const std::vector<bool>* is_null_first, size_t size_of_chunk_batch)
        : _sort_exprs(sort_exprs), _size_of_chunk_batch(size_of_chunk_batch) {
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

void ChunksSorter::setup_runtime(RuntimeProfile* profile, const std::string& parent_timer) {
    _build_timer = ADD_CHILD_TIMER(profile, "1-BuildingTime", parent_timer);
    _sort_timer = ADD_CHILD_TIMER(profile, "2-SortingTime", parent_timer);
    _merge_timer = ADD_CHILD_TIMER(profile, "3-MergingTime", parent_timer);
    _output_timer = ADD_CHILD_TIMER(profile, "4-OutputTime", parent_timer);
}

Status ChunksSorter::finish(RuntimeState* state) {
    RETURN_IF_ERROR(done(state));
    _is_sink_complete = true;
    return Status::OK();
}

bool ChunksSorter::sink_complete() {
    return _is_sink_complete;
}

} // namespace starrocks::vectorized
