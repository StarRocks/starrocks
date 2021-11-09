// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/crossjoin/cross_join_context.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

class CrossJoinRightSinkOperator final : public Operator {
public:
    CrossJoinRightSinkOperator(int32_t id, int32_t plan_node_id,
                               const std::shared_ptr<CrossJoinContext>& cross_join_context)
            : Operator(id, "cross_join_right_sink", plan_node_id), _cross_join_context(cross_join_context) {}

    ~CrossJoinRightSinkOperator() override = default;

    bool has_output() const override { return false; }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished; }

    void finish(RuntimeState* state) override {
        if (!_is_finished) {
            // Used to notify cross_join_left_operator.
            _cross_join_context->set_right_complete();
            _is_finished = true;
        }
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

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

private:
    std::shared_ptr<CrossJoinContext> _cross_join_context;
};

} // namespace starrocks::pipeline
