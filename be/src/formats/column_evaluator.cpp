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

#include "formats/column_evaluator.h"

#include "column/chunk.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {

Status ColumnEvaluator::init(const std::vector<std::unique_ptr<ColumnEvaluator>>& source) {
    for (const auto& e : source) {
        RETURN_IF_ERROR(e->init());
    }
    return Status::OK();
}

std::vector<std::unique_ptr<ColumnEvaluator>> ColumnEvaluator::clone(
        const std::vector<std::unique_ptr<ColumnEvaluator>>& source) {
    std::vector<std::unique_ptr<ColumnEvaluator>> es;
    es.reserve(source.size());
    for (const auto& e : source) {
        es.push_back(e->clone());
    }
    return es;
}

std::vector<TypeDescriptor> ColumnEvaluator::types(const std::vector<std::unique_ptr<ColumnEvaluator>>& source) {
    std::vector<TypeDescriptor> types;
    types.reserve(source.size());
    for (const auto& e : source) {
        types.push_back(e->type());
    }
    return types;
}

std::vector<std::unique_ptr<ColumnEvaluator>> ColumnExprEvaluator::from_exprs(const std::vector<TExpr>& exprs,
                                                                              RuntimeState* state) {
    std::vector<std::unique_ptr<ColumnEvaluator>> es;
    es.reserve(exprs.size());
    for (const auto& e : exprs) {
        es.push_back(std::make_unique<ColumnExprEvaluator>(e, state));
    }
    return es;
}

ColumnExprEvaluator::ColumnExprEvaluator(const TExpr& expr, RuntimeState* state) : _expr(expr), _state(state) {}

ColumnExprEvaluator::~ColumnExprEvaluator() {
    if (_expr_ctx) {
        _expr_ctx->close(_state);
    }
}

Status ColumnExprEvaluator::init() {
    if (_expr_ctx == nullptr) {
        RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), _expr, &_expr_ctx, _state));
        RETURN_IF_ERROR(_expr_ctx->prepare(_state));
        RETURN_IF_ERROR(_expr_ctx->open(_state));
    }
    return Status::OK();
}

std::unique_ptr<ColumnEvaluator> ColumnExprEvaluator::clone() const {
    return std::make_unique<ColumnExprEvaluator>(_expr, _state);
}

TypeDescriptor ColumnExprEvaluator::type() const {
    DCHECK(_expr_ctx != nullptr) << "not inited";
    return _expr_ctx->root()->type();
}

StatusOr<ColumnPtr> ColumnExprEvaluator::evaluate(Chunk* chunk) {
    DCHECK(_expr_ctx != nullptr) << "not inited";
    return _expr_ctx->evaluate(chunk);
}

std::vector<std::unique_ptr<ColumnEvaluator>> ColumnSlotIdEvaluator::from_types(
        const std::vector<TypeDescriptor>& types) {
    std::vector<std::unique_ptr<ColumnEvaluator>> es;
    es.reserve(types.size());
    for (size_t i = 0; i < types.size(); i++) {
        es.push_back(std::make_unique<ColumnSlotIdEvaluator>(i, types[i]));
    }
    return es;
}

Status ColumnSlotIdEvaluator::init() {
    return Status::OK();
}

std::unique_ptr<ColumnEvaluator> ColumnSlotIdEvaluator::clone() const {
    return std::make_unique<ColumnSlotIdEvaluator>(_slot_id, _type);
}

StatusOr<ColumnPtr> ColumnSlotIdEvaluator::evaluate(Chunk* chunk) {
    DCHECK(chunk->is_slot_exist(_slot_id));
    return chunk->get_column_by_slot_id(_slot_id);
}

} // namespace starrocks
