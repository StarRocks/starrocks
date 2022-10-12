// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "runtime/descriptors.h"

namespace starrocks {
class ExprContext;

namespace pipeline {
class SelectOperator final : public Operator {
public:
    SelectOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                   const std::vector<ExprContext*>& conjunct_ctxs)
            : Operator(factory, id, "select", plan_node_id, driver_sequence), _conjunct_ctxs(conjunct_ctxs) {}

    ~SelectOperator() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool has_output() const override { return _curr_chunk != nullptr || _pre_output_chunk != nullptr; }
    bool need_input() const override;
    bool is_finished() const override { return _is_finished && !_curr_chunk && !_pre_output_chunk; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    // _curr_chunk used to receive input chunks, and apply predicate filtering.
    vectorized::ChunkPtr _curr_chunk = nullptr;
    // _pre_output_chunk used to merge small _curr_chunk until it's big enough, then return as output.
    vectorized::ChunkPtr _pre_output_chunk = nullptr;

    const std::vector<ExprContext*>& _conjunct_ctxs;

    bool _is_finished = false;
};

class SelectOperatorFactory final : public OperatorFactory {
public:
    SelectOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<ExprContext*>&& conjunct_ctxs)
            : OperatorFactory(id, "select", plan_node_id), _conjunct_ctxs(std::move(conjunct_ctxs)) {}

    ~SelectOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SelectOperator>(this, _id, _plan_node_id, driver_sequence, _conjunct_ctxs);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::vector<ExprContext*> _conjunct_ctxs;
};

} // namespace pipeline
} // namespace starrocks
