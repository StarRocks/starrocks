// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateDistinctBlockingSinkOperator : public Operator {
public:
    AggregateDistinctBlockingSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                          int32_t driver_sequence, AggregatorPtr aggregator)
            : Operator(factory, id, "aggregate_distinct_blocking_sink", plan_node_id, driver_sequence),
              _aggregator(std::move(aggregator)) {
        _aggregator->set_aggr_phase(AggrPhase2);
        _aggregator->ref();
    }
    ~AggregateDistinctBlockingSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished || _aggregator->is_finished(); }
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    Status reset_state(RuntimeState* state, const std::vector<vectorized::ChunkPtr>& chunks) override;

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
                                                 AggregatorFactoryPtr aggregator_factory,
                                                 const std::vector<ExprContext*>& partition_by_exprs)
            : OperatorFactory(id, "aggregate_distinct_blocking_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)),
              _partition_by_exprs(partition_by_exprs) {}

    ~AggregateDistinctBlockingSinkOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(OperatorFactory::prepare(state));
        RETURN_IF_ERROR(Expr::prepare(_partition_by_exprs, state));
        RETURN_IF_ERROR(Expr::open(_partition_by_exprs, state));
        return Status::OK();
    }
    void close(RuntimeState* state) override {
        Expr::close(_partition_by_exprs, state);
        OperatorFactory::close(state);
    }
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateDistinctBlockingSinkOperator>(
                this, _id, _plan_node_id, driver_sequence, _aggregator_factory->get_or_create(driver_sequence));
    }

    std::vector<ExprContext*>& partition_by_exprs() { return _partition_by_exprs; }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
    std::vector<ExprContext*> _partition_by_exprs;
};
} // namespace starrocks::pipeline
