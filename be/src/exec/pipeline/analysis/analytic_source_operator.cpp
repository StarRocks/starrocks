// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "analytic_source_operator.h"

namespace starrocks::pipeline {

bool AnalyticSourceOperator::has_output() const {
    return !_analytor->is_chunk_buffer_empty();
}

bool AnalyticSourceOperator::is_finished() const {
    return _analytor->is_sink_complete() && _analytor->is_chunk_buffer_empty();
}

Status AnalyticSourceOperator::set_finished(RuntimeState* state) {
    return _analytor->set_finished();
}

void AnalyticSourceOperator::close(RuntimeState* state) {
    _analytor->unref(state);
    SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> AnalyticSourceOperator::pull_chunk(RuntimeState* state) {
    auto chunk = _analytor->poll_chunk_buffer();
    eval_runtime_bloom_filters(chunk.get());
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters({}, chunk.get()));
    return chunk;
}
} // namespace starrocks::pipeline
