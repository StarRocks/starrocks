// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/operator.h"

namespace starrocks {

class RuntimeState;

namespace pipeline {

// Accumulate chunks and output a chunk, until the number of rows of the input chunks is large enough.
class ChunkAccumulateOperator final : public Operator {
public:
    ChunkAccumulateOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "chunk_accumulate", plan_node_id, driver_sequence) {}

    ~ChunkAccumulateOperator() override = default;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool has_output() const override { return _out_chunk != nullptr || (_is_finished && _in_chunk != nullptr); }
    bool need_input() const override { return !_is_finished && _out_chunk == nullptr; }
    bool is_finished() const override { return _is_finished && _in_chunk == nullptr && _out_chunk == nullptr; }

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& chunks) override;

private:
    static constexpr double LOW_WATERMARK_ROWS_RATE = 0.75;          // 0.75 * chunk_size
    static constexpr size_t LOW_WATERMARK_BYTES = 256 * 1024 * 1024; // 256MB.

    bool _is_finished = false;
    vectorized::ChunkPtr _in_chunk = nullptr;
    vectorized::ChunkPtr _out_chunk = nullptr;
};

class ChunkAccumulateOperatorFactory final : public OperatorFactory {
public:
    ChunkAccumulateOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "chunk_accumulate", plan_node_id) {}

    ~ChunkAccumulateOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ChunkAccumulateOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

} // namespace pipeline
} // namespace starrocks
