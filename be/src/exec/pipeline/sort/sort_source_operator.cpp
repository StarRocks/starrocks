// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/sort/sort_source_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/mysql_result_writer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/stack_util.h"

namespace starrocks::pipeline {
Status SortSourceOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);

    return Status::OK();
}

Status SortSourceOperator::close(RuntimeState* state) {
    return Status::OK();
}

bool SortSourceOperator::need_input() {
    if (is_finished()) {
        return false;
    }
    return true;
}

StatusOr<vectorized::ChunkPtr> SortSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk;
    if (_chunks_sorter->pull_chunk(&chunk)) {
        is_source_complete = true;
    }

    if (chunk == nullptr) {
        return std::move(std::make_shared<vectorized::Chunk>());
    } else {
        return std::move(chunk);
    }
}

void SortSourceOperator::finish(RuntimeState* state) {
    _is_finished = true;
}

bool SortSourceOperator::has_output() {
    return _chunks_sorter->sink_complete();
}

bool SortSourceOperator::is_finished() const {
    return is_source_complete;
}

} // namespace starrocks::pipeline
