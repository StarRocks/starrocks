// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateBlockingSourceOperator : public SourceOperator {
public:
    AggregateBlockingSourceOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : SourceOperator(id, "aggregate_blocking_source_operator", plan_node_id), _aggregator(aggregator) {}
    ~AggregateBlockingSourceOperator() = default;

    bool has_output() const override;
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    AggregatorPtr _aggregator;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateBlockingSourceOperatorFactory final : public OperatorFactory {
public:
    AggregateBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : OperatorFactory(id, plan_node_id), _aggregator(aggregator) {}

    ~AggregateBlockingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<AggregateBlockingSourceOperator>(_id, _plan_node_id, _aggregator);
    }

private:
    AggregatorPtr _aggregator;
};
} // namespace starrocks::pipeline