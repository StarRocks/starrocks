// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "exec/pipeline/operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
// TODO: implements cache-relation method
class SortedAggregateStreamingSinkOperator : public Operator {
public:
    SortedAggregateStreamingSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence,
                                         std::shared_ptr<SortedStreamingAggregator> aggregator);
    ~SortedAggregateStreamingSinkOperator() override = default;

    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;
    std::shared_ptr<SortedStreamingAggregator> _aggregator;
};

class SortedAggregateStreamingSinkOperatorFactory final : public OperatorFactory {
public:
    SortedAggregateStreamingSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                StreamingAggregatorFactoryPtr aggregator_factory)
            : OperatorFactory(id, "aggregate_streaming_sink", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~SortedAggregateStreamingSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    StreamingAggregatorFactoryPtr _aggregator_factory = nullptr;
};

} // namespace starrocks::pipeline
