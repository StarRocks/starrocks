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

#include "aggregate_distinct_blocking_sink_operator.h"

#include "runtime/current_thread.h"

namespace starrocks::pipeline {

Status AggregateDistinctBlockingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    return _aggregator->open(state);
}

void AggregateDistinctBlockingSinkOperator::close(RuntimeState* state) {
    auto* counter = ADD_COUNTER(_unique_metrics, "HashTableMemoryUsage", TUnit::BYTES);
    counter->set(_aggregator->hash_set_memory_usage());
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateDistinctBlockingSinkOperator::set_finishing(RuntimeState* state) {
    if (_is_finished) return Status::OK();

    _is_finished = true;

    // skip processing if cancelled
    if (state->is_cancelled()) {
        return Status::Cancelled("runtime state is cancelled");
    }

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

    // If hash set is empty, we don't need to return value
    if (_aggregator->hash_set_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    _aggregator->hash_set_variant().visit(
            [&](auto& hash_set_with_key) { _aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });

    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<ChunkPtr> AggregateDistinctBlockingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AggregateDistinctBlockingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DCHECK_LE(chunk->num_rows(), state->chunk_size());
    {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        bool limit_with_no_agg = _aggregator->limit() != -1;
        if (limit_with_no_agg) {
            auto size = _aggregator->hash_set_variant().size();
            if (size >= _aggregator->limit()) {
                set_finishing(state);
                return Status::OK();
            }
        }
        RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set(chunk->num_rows()));
        TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());

        _aggregator->update_num_input_rows(chunk->num_rows());
    }

    return Status::OK();
}
Status AggregateDistinctBlockingSinkOperator::reset_state(RuntimeState* state,
                                                          const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    return _aggregator->reset_state(state, refill_chunks, this);
}
} // namespace starrocks::pipeline
