// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/operator.h"

namespace starrocks {
class ExprContext;
namespace pipeline {
class ProjectOperator final : public Operator {
public:
    ProjectOperator(int32_t id, int32_t plan_node_id, std::vector<int32_t>& column_ids,
                    const std::vector<ExprContext*>& expr_ctxs, const std::vector<bool>& type_is_nullable,
                    const std::vector<int32_t>& common_sub_column_ids,
                    const std::vector<ExprContext*>& common_sub_expr_ctxs)
            : Operator(id, "project", plan_node_id),
              _column_ids(column_ids),
              _expr_ctxs(expr_ctxs),
              _type_is_nullable(type_is_nullable),
              _common_sub_column_ids(common_sub_column_ids),
              _common_sub_expr_ctxs(common_sub_expr_ctxs) {}

    ~ProjectOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override { return _cur_chunk != nullptr; }

    bool need_input() const override { return !_is_finished && _cur_chunk == nullptr; }

    bool is_finished() const override { return _is_finished && _cur_chunk == nullptr; }

    void finish(RuntimeState* state) override { _is_finished = true; }

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;

private:
    const std::vector<int32_t>& _column_ids;
    const std::vector<ExprContext*>& _expr_ctxs;
    const std::vector<bool>& _type_is_nullable;

    const std::vector<int32_t>& _common_sub_column_ids;
    const std::vector<ExprContext*>& _common_sub_expr_ctxs;

    bool _is_finished = false;
    vectorized::ChunkPtr _cur_chunk = nullptr;
};

class ProjectOperatorFactory final : public OperatorFactory {
public:
    ProjectOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<int32_t>&& column_ids,
                           std::vector<ExprContext*>&& expr_ctxs, std::vector<bool>&& type_is_nullable,
                           std::vector<int32_t>&& common_sub_column_ids,
                           std::vector<ExprContext*>&& common_sub_expr_ctxs)
            : OperatorFactory(id, "project", plan_node_id),
              _column_ids(std::move(column_ids)),
              _expr_ctxs(std::move(expr_ctxs)),
              _type_is_nullable(std::move(type_is_nullable)),
              _common_sub_column_ids(std::move(common_sub_column_ids)),
              _common_sub_expr_ctxs(std::move(common_sub_expr_ctxs)) {}

    ~ProjectOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ProjectOperator>(_id, _plan_node_id, _column_ids, _expr_ctxs, _type_is_nullable,
                                                 _common_sub_column_ids, _common_sub_expr_ctxs);
    }

    Status prepare(RuntimeState* state, MemTracker* mem_tracker);
    void close(RuntimeState* state);

private:
    std::vector<int32_t> _column_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;

    std::vector<int32_t> _common_sub_column_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;
};

} // namespace pipeline
} // namespace starrocks
