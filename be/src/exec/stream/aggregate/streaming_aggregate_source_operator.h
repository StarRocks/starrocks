// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class StreamingAggregateSourceOperator : public SourceOperator {
public:
    StreamingAggregateSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                     int32_t driver_sequence, AggregatorPtr aggregator)
            : SourceOperator(factory, id, "streaming_aggregate_source", plan_node_id, driver_sequence),
              _aggregator(std::move(aggregator)) {
        _aggregator->ref();
    }

    ~StreamingAggregateSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    void _output_chunk_from_hash_map(vectorized::ChunkPtr* chunk, RuntimeState* state);

    // It is used to perform aggregation algorithms shared by
    // StreamingAggregateSinkOperator. It is
    // - prepared at SinkOperator::prepare(),
    // - reffed at constructor() of both sink and source operator,
    // - unreffed at close() of both sink and source operator.
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    mutable bool _is_finished = false;
};

class StreamingAggregateSourceOperatorFactory final : public SourceOperatorFactory {
public:
    StreamingAggregateSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "streaming_aggregate_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~StreamingAggregateSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<StreamingAggregateSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                  _aggregator_factory->get_or_create(driver_sequence));
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
};
} // namespace starrocks::pipeline
