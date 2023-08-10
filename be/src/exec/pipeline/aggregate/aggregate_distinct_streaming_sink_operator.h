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

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/aggregator.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {
// TODO: think about refactor
class AggregateDistinctStreamingSinkOperator : public Operator {
public:
    AggregateDistinctStreamingSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                           int32_t driver_sequence, AggregatorPtr aggregator)
            : Operator(factory, id, "aggregate_distinct_streaming_sink", plan_node_id, false, driver_sequence),
              _aggregator(std::move(aggregator)) {
        _aggregator->set_aggr_phase(AggrPhase1);
        _aggregator->ref();
    }
    ~AggregateDistinctStreamingSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override {
        return !is_finished() && !_aggregator->is_streaming_all_states() && !_aggregator->is_chunk_buffer_full();
    }
    bool is_finished() const override { return _is_finished || _aggregator->is_finished(); }
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

    bool releaseable() const override { return true; }
    void set_execute_mode(int performance_level) override;

private:
    // Invoked by push_chunk if current mode is TStreamingPreaggregationMode::FORCE_STREAMING
    Status _push_chunk_by_force_streaming(const ChunkPtr& chunk);

    // Invoked by push_chunk  if current mode is TStreamingPreaggregationMode::FORCE_PREAGGREGATION
    Status _push_chunk_by_force_preaggregation(const size_t chunk_size);

    // Invoked by push_chunk  if current mode is TStreamingPreaggregationMode::AUTO
    Status _push_chunk_by_auto(const ChunkPtr& chunk, const size_t chunk_size);

    // Invoked by push_chunk  if current mode is TStreamingPreaggregationMode::LIMITED
    Status _push_chunk_by_limited_memory(const ChunkPtr& chunk, const size_t chunk_size);

    // It is used to perform aggregation algorithms shared by
    // AggregateDistinctStreamingSourceOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
    LimitedMemAggState _limited_mem_state;
};

class AggregateDistinctStreamingSinkOperatorFactory final : public OperatorFactory {
public:
    AggregateDistinctStreamingSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                  AggregatorFactoryPtr aggregator_factory)
            : OperatorFactory(id, "aggregate_distinct_streaming_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~AggregateDistinctStreamingSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateDistinctStreamingSinkOperator>(
                this, _id, _plan_node_id, driver_sequence, _aggregator_factory->get_or_create(driver_sequence));
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
};
} // namespace starrocks::pipeline
