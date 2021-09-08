// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "aggregate_base_operator.h"

namespace starrocks::pipeline {

class AggregateStreamingOperator final : public AggregateBaseOperator {
public:
    AggregateStreamingOperator(int32_t id, int32_t plan_node_id, const TPlanNode& tnode)
            : AggregateBaseOperator(id, "aggregate_streaming", plan_node_id, tnode) {}
    ~AggregateStreamingOperator() override = default;

    bool has_output() const override;
    bool is_finished() const override;
    void finish(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    Status _push_chunk_by_force_streaming();
    Status _push_chunk_by_force_preaggregation(const size_t chunk_size);
    Status _push_chunk_by_auto(const size_t chunk_size);
    void _output_chunk_from_hash_map(vectorized::ChunkPtr* chunk);

    vectorized::ChunkPtr _curr_chunk = nullptr;
};

class AggregateStreamingOperatorFactory final : public AggregateBaseOperatorFactory {
public:
    AggregateStreamingOperatorFactory(int32_t id, int32_t plan_node_id, const TPlanNode& tnode)
            : AggregateBaseOperatorFactory(id, plan_node_id, tnode) {}

    ~AggregateStreamingOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<AggregateStreamingOperator>(_id, _plan_node_id, _tnode);
    }
};
} // namespace starrocks::pipeline