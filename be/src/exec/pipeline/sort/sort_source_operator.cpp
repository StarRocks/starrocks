// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/sort/sort_source_operator.h"

#include "column/chunk.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
StatusOr<vectorized::ChunkPtr> SortSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk;
    if (_chunks_sorter->pull_chunk(&chunk)) {
        _is_source_complete = true;
    }

    if (!chunk) {
        return std::make_shared<vectorized::Chunk>();
    } else {
        return std::move(chunk);
    }
}

void SortSourceOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
}

bool SortSourceOperator::has_output() const {
    return _chunks_sorter->sink_complete();
}

bool SortSourceOperator::is_finished() const {
    return _is_source_complete;
}

} // namespace starrocks::pipeline
