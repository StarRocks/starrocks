// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateDistinctBlockingSinkOperator : public Operator {
public:
    AggregateDistinctBlockingSinkOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : Operator(id, "aggregate_distinct_blocking_sink", plan_node_id), _aggregator(std::move(aggregator)) {
        _aggregator->set_aggr_phase(AggrPhase2);
        _aggregator->ref();
    }
    ~AggregateDistinctBlockingSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished || _aggregator->is_finished(); }
    void set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    // It is used to perform aggregation algorithms shared by
    // AggregateDistinctBlockingSourceOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateDistinctBlockingSinkOperatorFactory final : public OperatorFactory {
public:
    AggregateDistinctBlockingSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                 AggregatorFactoryPtr aggregator_factory)
            : OperatorFactory(id, "aggregate_distinct_blocking_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~AggregateDistinctBlockingSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateDistinctBlockingSinkOperator>(
                _id, _plan_node_id, _aggregator_factory->get_or_create(driver_sequence));
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
};
} // namespace starrocks::pipeline
