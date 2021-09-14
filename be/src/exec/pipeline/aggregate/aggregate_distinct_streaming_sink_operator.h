// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateDistinctStreamingSinkOperator : public Operator {
public:
    AggregateDistinctStreamingSinkOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : Operator(id, "aggregate_distinct_streaming_sink", plan_node_id), _aggregator(aggregator) {
        _aggregator->set_aggr_phase(AggrPhase1);
    }
    ~AggregateDistinctStreamingSinkOperator() = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    // Invoked by push_chunk if current mode is TStreamingPreaggregationMode::FORCE_STREAMING
    Status _push_chunk_by_force_streaming();

    // Invoked by push_chunk  if current mode is TStreamingPreaggregationMode::FORCE_PREAGGREGATION
    Status _push_chunk_by_force_preaggregation(const size_t chunk_size);

    // Invoked by push_chunk  if current mode is TStreamingPreaggregationMode::AUTO
    Status _push_chunk_by_auto(const size_t chunk_size);

    // It is used to perform aggregation algorithms
    // shared by AggregateStreamingSourceOperator
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateDistinctStreamingSinkOperatorFactory final : public OperatorFactory {
public:
    AggregateDistinctStreamingSinkOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : OperatorFactory(id, plan_node_id), _aggregator(aggregator) {}

    ~AggregateDistinctStreamingSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<AggregateDistinctStreamingSinkOperator>(_id, _plan_node_id, _aggregator);
    }

private:
    AggregatorPtr _aggregator = nullptr;
};
} // namespace starrocks::pipeline