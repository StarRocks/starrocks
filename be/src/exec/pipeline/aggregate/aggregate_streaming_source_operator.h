// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateStreamingSourceOperator : public SourceOperator {
public:
    AggregateStreamingSourceOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : SourceOperator(id, "aggregate_streaming_source", plan_node_id), _aggregator(std::move(aggregator)) {}
    ~AggregateStreamingSourceOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    void _output_chunk_from_hash_map(vectorized::ChunkPtr* chunk);

    // It is used to perform aggregation algorithms
    // shared by AggregateStreamingSinkOperator
    AggregatorPtr _aggregator = nullptr;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateStreamingSourceOperatorFactory final : public SourceOperatorFactory {
public:
    AggregateStreamingSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorFactoryPtr aggregator_factory)
            : SourceOperatorFactory(id, "aggregate_streaming_source", plan_node_id),
              _aggregator_factory(std::move(aggregator_factory)) {}

    ~AggregateStreamingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        size_t expected_idx = _aggregator_idx.load(std::memory_order_acquire);
        while (!_aggregator_idx.compare_exchange_weak(expected_idx, expected_idx + 1))
            ;
        return std::make_shared<AggregateStreamingSourceOperator>(_id, _plan_node_id,
                                                                  _aggregator_factory->get_or_create(expected_idx + 1));
    }

private:
    AggregatorFactoryPtr _aggregator_factory = nullptr;
    std::atomic_size_t _aggregator_idx = 0;
};
} // namespace starrocks::pipeline
