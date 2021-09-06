// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "aggregate_base_operator.h"

namespace starrocks::pipeline {
class AggregateBlockingOperator final : public AggregateBaseOperator {
public:
    AggregateBlockingOperator(int32_t id, int32_t plan_node_id, const TPlanNode& tnode)
            : AggregateBaseOperator(id, "aggregate_blocking", plan_node_id, tnode) {
        _aggr_phase = vectorized::AggrPhase::AggrPhase2;
    }
    ~AggregateBlockingOperator() override = default;

    bool has_output() override { return _is_pre_finished && !_is_finished; }
    bool need_input() override { return true; }
    bool is_finished() const override { return _is_finished; }
    void finish(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_pre_finished = false;
};

class AggregateBlockingOperatorFactory final : public AggregateBaseOperatorFactory {
public:
    AggregateBlockingOperatorFactory(int32_t id, int32_t plan_node_id, const TPlanNode& tnode)
            : AggregateBaseOperatorFactory(id, plan_node_id, tnode) {}

    ~AggregateBlockingOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<AggregateBlockingOperator>(_id, _plan_node_id, _tnode);
    }
};
} // namespace starrocks::pipeline