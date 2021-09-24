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
              _aggregator(std::move(aggregator)) {}
    ~AggregateDistinctStreamingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    void _output_chunk_from_hash_set(vectorized::ChunkPtr* chunk);

    // It is used to perform aggregation algorithms
    // shared by AggregateStreamingSinkOperator
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateDistinctStreamingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AggregateDistinctStreamingSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : SourceOperatorFactory(id, plan_node_id), _aggregator(std::move(aggregator)) {}

    ~AggregateDistinctStreamingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<AggregateDistinctStreamingSourceOperator>(_id, _plan_node_id, _aggregator);
    }

private:
    AggregatorPtr _aggregator = nullptr;
};
} // namespace starrocks::pipeline
