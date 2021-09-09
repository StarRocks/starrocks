// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/aggregator.h"

namespace starrocks::pipeline {
class AggregateStreamingSourceOperator : public SourceOperator {
public:
    AggregateStreamingSourceOperator(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : SourceOperator(id, "aggregate_streaming_source_operator", plan_node_id), _aggregator(aggregator) {}
    ~AggregateStreamingSourceOperator() = default;

    bool has_output() const override;
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    void _output_chunk_from_hash_map(vectorized::ChunkPtr* chunk);

    AggregatorPtr _aggregator;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class AggregateStreamingSourceOperatorFactory final : public OperatorFactory {
public:
    AggregateStreamingSourceOperatorFactory(int32_t id, int32_t plan_node_id, AggregatorPtr aggregator)
            : OperatorFactory(id, plan_node_id), _aggregator(aggregator) {}

    ~AggregateStreamingSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<AggregateStreamingSourceOperator>(_id, _plan_node_id, _aggregator);
    }

private:
    AggregatorPtr _aggregator;
};
} // namespace starrocks::pipeline