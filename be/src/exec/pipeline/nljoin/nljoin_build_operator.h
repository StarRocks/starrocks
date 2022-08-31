// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

// NLJoinBuildOperator
// Collect data of right table into the cross-join-context
class NLJoinBuildOperator final : public Operator {
public:
    NLJoinBuildOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence,
                        const std::shared_ptr<NLJoinContext>& cross_join_context)
            : Operator(factory, id, "nestloop_join_build", plan_node_id, driver_sequence),
              _cross_join_context(cross_join_context) {
        _cross_join_context->ref();
    }

    ~NLJoinBuildOperator() override = default;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished || _cross_join_context->is_finished(); }

    Status set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    bool _is_finished = false;

    const std::shared_ptr<NLJoinContext>& _cross_join_context;
};

class NLJoinBuildOperatorFactory final : public OperatorFactory {
public:
    NLJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<NLJoinContext> cross_join_context)
            : OperatorFactory(id, "nljoin_build", plan_node_id), _cross_join_context(std::move(cross_join_context)) {}

    ~NLJoinBuildOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<NLJoinBuildOperator>(this, _id, _plan_node_id, driver_sequence, _cross_join_context);
    }

private:
    std::shared_ptr<NLJoinContext> _cross_join_context;
};

} // namespace starrocks::pipeline
