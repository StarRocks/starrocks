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
class CrossJoinContext;
class CrossJoinRightSinkOperator final : public Operator {
public:
    CrossJoinRightSinkOperator(int32_t id, int32_t plan_node_id,
                               const std::shared_ptr<CrossJoinContext>& cross_join_context)
            : Operator(id, "cross_join_right_sink", plan_node_id), _cross_join_context(cross_join_context) {}

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

    const std::shared_ptr<CrossJoinContext>& _cross_join_context;
};

class CrossJoinRightSinkOperatorFactory final : public OperatorFactory {
public:
    CrossJoinRightSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                      std::shared_ptr<CrossJoinContext> cross_join_context)
            : OperatorFactory(id, "cross_join_right_sink", plan_node_id), _cross_join_context(cross_join_context) {}

    ~CrossJoinRightSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        auto ope = std::make_shared<CrossJoinRightSinkOperator>(_id, _plan_node_id, _cross_join_context);
        return ope;
    }

    Status prepare(RuntimeState* state, MemTracker* mem_tracker) override;
    void close(RuntimeState* state) override;

private:
    std::shared_ptr<CrossJoinContext> _cross_join_context;
};

} // namespace pipeline
} // namespace starrocks
