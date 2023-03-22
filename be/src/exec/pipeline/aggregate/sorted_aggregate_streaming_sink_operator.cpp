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

#include "exec/pipeline/aggregate/sorted_aggregate_streaming_sink_operator.h"

#include "exec/sorted_streaming_aggregator.h"
#include "runtime/current_thread.h"

namespace starrocks::pipeline {
SortedAggregateStreamingSinkOperator::SortedAggregateStreamingSinkOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
        std::shared_ptr<SortedStreamingAggregator> aggregator)
        : Operator(factory, id, "sorted_aggregate_streaming_sink", plan_node_id, driver_sequence),
          _aggregator(std::move(aggregator)) {
    _aggregator->ref();
}

Status SortedAggregateStreamingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    _accumulator.set_max_size(state->chunk_size());
    return _aggregator->open(state);
}

void SortedAggregateStreamingSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

bool SortedAggregateStreamingSinkOperator::need_input() const {
    return !is_finished() && _aggregator->chunk_buffer_size() < Aggregator::MAX_CHUNK_BUFFER_SIZE;
}

bool SortedAggregateStreamingSinkOperator::is_finished() const {
    return _is_finished || _aggregator->is_finished();
}

Status SortedAggregateStreamingSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    ASSIGN_OR_RETURN(auto res, _aggregator->pull_eos_chunk());
    DCHECK(_accumulator.need_input());
    if (res && !res->is_empty()) {
        _accumulator.push(std::move(res));
    }
    _accumulator.finalize();
    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        _aggregator->offer_chunk_to_buffer(accumulated);
    }

    _aggregator->set_ht_eos();
    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<ChunkPtr> SortedAggregateStreamingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status SortedAggregateStreamingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();
    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));
    RETURN_IF_ERROR(_aggregator->evaluate_agg_fn_exprs(chunk.get()));
    ASSIGN_OR_RETURN(auto res, _aggregator->streaming_compute_agg_state(chunk_size));
    DCHECK(_accumulator.need_input());
    if (!res->is_empty()) {
        _accumulator.push(std::move(res));
    }
    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        _aggregator->offer_chunk_to_buffer(accumulated);
    }
    DCHECK(_accumulator.need_input());

    return Status::OK();
}

OperatorPtr SortedAggregateStreamingSinkOperatorFactory::create(int32_t degree_of_parallelism,
                                                                int32_t driver_sequence) {
    return std::make_shared<SortedAggregateStreamingSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                  _aggregator_factory->get_or_create(driver_sequence));
}

} // namespace starrocks::pipeline
