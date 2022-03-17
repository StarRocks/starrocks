// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateStreamingSourceOperator : public SourceOperator {
public:
    AggregateStreamingSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                     AggregatorPtr aggregator)
            : SourceOperator(factory, id, "aggregate_streaming_source", plan_node_id),
              _aggregator(std::move(aggregator)) {
        _aggregator->ref();
    }

    ~AggregateStreamingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    void _output_chunk_from_hash_map(vectorized::ChunkPtr* chunk, RuntimeState* state);

    // It is used to perform aggregation algorithms shared by
    // AggregateStreamingSinkOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    mutable bool _is_finished = false;
};

class AggregateStreamingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AggregateStreamingSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "aggregate_streaming_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~AggregateStreamingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateStreamingSourceOperator>(this, _id, _plan_node_id,
                                                                  _aggregator_factory->get_or_create(driver_sequence));
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
};
} // namespace starrocks::pipeline
