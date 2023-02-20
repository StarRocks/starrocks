// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

StatusOr<ChunkPtr> AnalyticSourceOperator::pull_chunk(RuntimeState* state) {
    auto chunk = _analytor->poll_chunk_buffer();
    eval_runtime_bloom_filters(chunk.get());
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters({}, chunk.get()));
    return chunk;
}
} // namespace starrocks::pipeline
