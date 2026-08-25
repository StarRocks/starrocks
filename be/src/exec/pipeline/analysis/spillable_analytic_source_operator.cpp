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

#include "spillable_analytic_source_operator.h"

#include "exec/pipeline/exchange/mem_limited_chunk_queue.h"

namespace starrocks::pipeline {

void SpillableAnalyticSourceOperator::close(RuntimeState* state) {
    if (_analytor->partition_streams_enabled()) {
        _analytor->descriptor_queue().close_consumer();
        _analytor->input_run()->close_consumer(0);
    }
    AnalyticSourceOperator::close(state);
}

bool SpillableAnalyticSourceOperator::has_output() const {
    if (!_analytor->partition_streams_enabled()) {
        return AnalyticSourceOperator::has_output();
    }
    // No sink-complete barrier: a descriptor is the read permit for its rows,
    // so this operator streams the sealed prefix while the sink keeps
    // producing. A permit and the covering input rows must both make
    // progress: resident -> true; on disk -> async load submitted, false
    // until loaded; drained -> true (pop returns EndOfFile); IO error ->
    // true (surfaces through pull).
    return _analytor->store_has_output();
}

bool SpillableAnalyticSourceOperator::is_finished() const {
    if (!_analytor->partition_streams_enabled()) {
        return AnalyticSourceOperator::is_finished();
    }
    return _analytor->store_eos() || _analytor->reached_limit() || _analytor->is_finished();
}

StatusOr<ChunkPtr> SpillableAnalyticSourceOperator::pull_chunk(RuntimeState* state) {
    if (!_analytor->partition_streams_enabled()) {
        return AnalyticSourceOperator::pull_chunk(state);
    }
    // A fully authorized chunk is replayed by sharing the input run's data
    // columns, which the queue and an in-flight flush task still reference. The
    // eval_* calls below filter in place, so sharing is only safe while this
    // operator has nothing to filter with. The flag is sticky because
    // eval_conjuncts_and_in_filters caches its predicate list on first use and
    // never drops it, whereas runtime_in_filters() is rebound (and cleared) by
    // set_precondition_ready -- once a filter has appeared, keep copying.
    const auto* bloom_filters = _runtime_access->get_runtime_bloom_filters();
    _output_may_be_filtered |= !runtime_in_filters().empty() ||
                               (bloom_filters != nullptr && !bloom_filters->empty()) ||
                               !_runtime_access->get_filter_null_value_columns().empty();

    ASSIGN_OR_RETURN(auto chunk,
                     _analytor->store_pull_chunk(state, /*allow_share_columns=*/!_output_may_be_filtered));
    if (chunk != nullptr && !chunk->is_empty()) {
        [[maybe_unused]] const size_t rows_before = chunk->num_rows();
        eval_runtime_bloom_filters(chunk.get());
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters({}, chunk.get()));
        DCHECK(_output_may_be_filtered || chunk->num_rows() == rows_before)
                << "a replay chunk sharing the input run's columns must not be filtered in place";
    }
    return chunk;
}

} // namespace starrocks::pipeline
