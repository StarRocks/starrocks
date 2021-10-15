// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"

namespace starrocks {
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class ExecNode;

namespace vectorized {
class ChunksSorter;
}

namespace pipeline {
class CrossJoinRightSinkOperator final : public Operator {
public:
    CrossJoinRightSinkOperator(int32_t id, int32_t plan_node_id, vectorized::ChunkPtr* build_chunk_ptr,
                               std::atomic<bool>* right_table_complete_ptr)
            : Operator(id, "cross_join_right_sink", plan_node_id),
              _build_chunk_ptr(build_chunk_ptr),
              _right_table_complete_ptr(right_table_complete_ptr) {}

    ~CrossJoinRightSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override { return _is_finished; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

    void finish(RuntimeState* state) override;

private:
    bool _is_finished = false;
    // Reference right table's data
    vectorized::ChunkPtr* _build_chunk_ptr = nullptr;
    // Used to mark that the right table has been constructed
    std::atomic<bool>* _right_table_complete_ptr = nullptr;
};

class CrossJoinRightSinkOperatorFactory final : public OperatorFactory {
public:
    CrossJoinRightSinkOperatorFactory(int32_t id, int32_t plan_node_id, vectorized::ChunkPtr* build_chunk_ptr,
                                      std::atomic<bool>* right_table_complete_ptr)
            : OperatorFactory(id, "cross_join_right_sink", plan_node_id),
              _build_chunk_ptr(build_chunk_ptr),
              _right_table_complete_ptr(right_table_complete_ptr) {}

    ~CrossJoinRightSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto ope = std::make_shared<CrossJoinRightSinkOperator>(_id, _plan_node_id, _build_chunk_ptr,
                                                                _right_table_complete_ptr);
        return ope;
    }

    Status prepare(RuntimeState* state, MemTracker* mem_tracker) override;
    void close(RuntimeState* state) override;

private:
    // Reference right table's data
    vectorized::ChunkPtr* _build_chunk_ptr = nullptr;
    // Used to mark that the right table has been constructed
    std::atomic<bool>* _right_table_complete_ptr = nullptr;
};

} // namespace pipeline
} // namespace starrocks
