// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "analytic_source_operator.h"

namespace starrocks::pipeline {

bool AnalyticSourceOperator::has_output() const {
    return _analytor->is_sink_complete() && !_analytor->is_chunk_buffer_empty();
}

bool AnalyticSourceOperator::is_finished() const {
    return _analytor->is_sink_complete() && _analytor->is_chunk_buffer_empty();
}

void AnalyticSourceOperator::finish(RuntimeState* state) {
    _is_finished = true;
}

Status AnalyticSourceOperator::close(RuntimeState* state) {
    // _analytor is shared by sink operator and source operator
    // we must only close it at source operator
    RETURN_IF_ERROR(_analytor->close(state));
    return SourceOperator::close(state);
}

StatusOr<vectorized::ChunkPtr> AnalyticSourceOperator::pull_chunk(RuntimeState* state) {
    return _analytor->poll_chunk_buffer();
}
} // namespace starrocks::pipeline
