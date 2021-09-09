// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateBlockingSinkOperator : public Operator {
public:
    AggregateBlockingSinkOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : Operator(id, "aggregate_blocking_sink_operator", plan_node_id), _aggregator(aggregator) {}
    ~AggregateBlockingSinkOperator() = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return true; }
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    AggregatorPtr _aggregator;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateBlockingSinkOperatorFactory final : public OperatorFactory {
public:
    AggregateBlockingSinkOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : OperatorFactory(id, plan_node_id), _aggregator(aggregator) {}

    ~AggregateBlockingSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<AggregateBlockingSinkOperator>(_id, _plan_node_id, _aggregator);
    }

private:
    AggregatorPtr _aggregator;
};
} // namespace starrocks::pipeline