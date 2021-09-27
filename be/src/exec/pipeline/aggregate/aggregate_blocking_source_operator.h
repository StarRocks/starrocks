// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateBlockingSourceOperator : public SourceOperator {
public:
    AggregateBlockingSourceOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : SourceOperator(id, "aggregate_blocking_source", plan_node_id), _aggregator(std::move(aggregator)) {}
    ~AggregateBlockingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    // It is used to perform aggregation algorithms
    // shared by AggregateBlockingSinkOperator
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateBlockingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AggregateBlockingSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : SourceOperatorFactory(id, plan_node_id), _aggregator(std::move(aggregator)) {}

    ~AggregateBlockingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateBlockingSourceOperator>(_id, _plan_node_id, _aggregator);
    }

private:
    AggregatorPtr _aggregator = nullptr;
};
} // namespace starrocks::pipeline
