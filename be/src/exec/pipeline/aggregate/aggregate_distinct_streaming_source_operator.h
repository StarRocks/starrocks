// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateDistinctStreamingSourceOperator : public SourceOperator {
public:
    AggregateDistinctStreamingSourceOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : SourceOperator(id, "aggregate_distinct_streaming_source", plan_node_id),
              _aggregator(std::move(aggregator)) {
        _aggregator->ref_no_barrier();
    }

    ~AggregateDistinctStreamingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;

    void set_finished(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    void _output_chunk_from_hash_set(vectorized::ChunkPtr* chunk);

    // It is used to perform aggregation algorithms shared by
    // AggregateDistinctStreamingSinkOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
};

class AggregateDistinctStreamingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AggregateDistinctStreamingSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                    AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "aggregate_distinct_streaming_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~AggregateDistinctStreamingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateDistinctStreamingSourceOperator>(
                _id, _plan_node_id, _aggregator_factory->get_or_create(driver_sequence));
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
};
} // namespace starrocks::pipeline
