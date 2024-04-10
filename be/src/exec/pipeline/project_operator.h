// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "exec/pipeline/operator.h"
#include "runtime/global_dict/parser.h"

namespace starrocks {
class ExprContext;
namespace pipeline {
class ProjectOperator final : public Operator {
public:
    ProjectOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                    std::vector<int32_t>& column_ids, const std::vector<ExprContext*>& expr_ctxs,
                    const std::vector<bool>& type_is_nullable, const std::vector<int32_t>& common_sub_column_ids,
                    const std::vector<ExprContext*>& common_sub_expr_ctxs)
            : Operator(factory, id, "project", plan_node_id, false, driver_sequence),
              _column_ids(column_ids),
              _expr_ctxs(expr_ctxs),
              _type_is_nullable(type_is_nullable),
              _common_sub_column_ids(common_sub_column_ids),
              _common_sub_expr_ctxs(common_sub_expr_ctxs) {}

    ~ProjectOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return _cur_chunk != nullptr; }

    bool need_input() const override { return !_is_finished && _cur_chunk == nullptr; }

    bool is_finished() const override { return _is_finished && _cur_chunk == nullptr; }

    bool ignore_empty_eos() const override { return false; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    const std::vector<int32_t>& _column_ids;
    const std::vector<ExprContext*>& _expr_ctxs;
    const std::vector<bool>& _type_is_nullable;

    const std::vector<int32_t>& _common_sub_column_ids;
    const std::vector<ExprContext*>& _common_sub_expr_ctxs;

    bool _is_finished = false;
    ChunkPtr _cur_chunk = nullptr;

    RuntimeProfile::Counter* _expr_compute_timer = nullptr;
    RuntimeProfile::Counter* _common_sub_expr_compute_timer = nullptr;
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
        return std::make_shared<ProjectOperator>(this, _id, _plan_node_id, driver_sequence, _column_ids, _expr_ctxs,
                                                 _type_is_nullable, _common_sub_column_ids, _common_sub_expr_ctxs);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::vector<int32_t> _column_ids;
    std::vector<ExprContext*> _expr_ctxs;
    std::vector<bool> _type_is_nullable;

    std::vector<int32_t> _common_sub_column_ids;
    std::vector<ExprContext*> _common_sub_expr_ctxs;
};

} // namespace pipeline
} // namespace starrocks
