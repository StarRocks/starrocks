// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/limit_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

StatusOr<vectorized::ChunkPtr> LimitOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

Status LimitOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _cur_chunk = chunk;

    int64_t old_limit;
    int64_t num_consume_rows;
    do {
        old_limit = _limit.load(std::memory_order_relaxed);
        num_consume_rows = std::min(old_limit, static_cast<int64_t>(chunk->num_rows()));
    } while (num_consume_rows && !_limit.compare_exchange_strong(old_limit, old_limit - num_consume_rows));

    if (num_consume_rows != chunk->num_rows()) {
        _cur_chunk->set_num_rows(num_consume_rows);
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
