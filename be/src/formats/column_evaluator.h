//
// Created by Letian Jiang on 2024/1/30.
//

#pragma once

#include "column/chunk.h"
#include "column/column.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// a convenience class to abstract away complexities of handling expr and its context
class ColumnEvaluator {
public:
    static Status init(const std::vector<std::unique_ptr<ColumnEvaluator>>& source) {
        for (const auto& e : source) {
            RETURN_IF_ERROR(e->init());
        }
        return Status::OK();
    }

    static std::vector<std::unique_ptr<ColumnEvaluator>> clone(
            const std::vector<std::unique_ptr<ColumnEvaluator>>& source) {
        std::vector<std::unique_ptr<ColumnEvaluator>> es;
        es.reserve(source.size());
        for (const auto& e : source) {
            es.push_back(e->clone());
        }
        return es;
    }

    static std::vector<TypeDescriptor> types(const std::vector<std::unique_ptr<ColumnEvaluator>>& source) {
        std::vector<TypeDescriptor> types;
        types.reserve(source.size());
        for (const auto& e : source) {
            types.push_back(e->type());
        }
        return types;
    }

    virtual ~ColumnEvaluator() = default;

    // idempotent
    virtual Status init() = 0;

    virtual std::unique_ptr<ColumnEvaluator> clone() const = 0;

    // requires inited
    virtual TypeDescriptor type() const = 0;

    // requires inited
    virtual StatusOr<ColumnPtr> evaluate(Chunk* chunk) = 0;
};

class ColumnExprEvaluator : public ColumnEvaluator {
public:
    static std::vector<std::unique_ptr<ColumnEvaluator>> from_exprs(const std::vector<TExpr>& exprs,
                                                                    RuntimeState* state) {
        std::vector<std::unique_ptr<ColumnEvaluator>> es;
        es.reserve(exprs.size());
        for (const auto& e : exprs) {
            es.push_back(std::make_unique<ColumnExprEvaluator>(e, state));
        }
        return es;
    }

    ColumnExprEvaluator(const TExpr& expr, RuntimeState* state) : _expr(expr), _state(state) {}

    ~ColumnExprEvaluator() override {
        if (_expr_ctx) {
            _expr_ctx->close(_state);
        }
    }

    Status init() override {
        if (_expr_ctx == nullptr) {
            RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), _expr, &_expr_ctx, _state));
            RETURN_IF_ERROR(_expr_ctx->prepare(_state));
            RETURN_IF_ERROR(_expr_ctx->open(_state));
        }
        return Status::OK();
    }

    std::unique_ptr<ColumnEvaluator> clone() const override {
        return std::make_unique<ColumnExprEvaluator>(_expr, _state);
    }

    TypeDescriptor type() const override {
        DCHECK(_expr_ctx != nullptr) << "not inited";
        return _expr_ctx->root()->type();
    }

    StatusOr<ColumnPtr> evaluate(Chunk* chunk) override {
        DCHECK(_expr_ctx != nullptr) << "not inited";
        return _expr_ctx->evaluate(chunk);
    }

private:
    TExpr _expr;
    ExprContext* _expr_ctx = nullptr;
    RuntimeState* _state;
};

// used for UT, since it is too hard to mock TExpr :(
class ColumnSlotIdEvaluator : public ColumnEvaluator {
public:
    static std::vector<std::unique_ptr<ColumnEvaluator>> from_types(const std::vector<TypeDescriptor>& types) {
        std::vector<std::unique_ptr<ColumnEvaluator>> es;
        es.reserve(types.size());
        for (size_t i = 0; i < types.size(); i++) {
            es.push_back(std::make_unique<ColumnSlotIdEvaluator>(i, types[i]));
        }
        return es;
    }

    ColumnSlotIdEvaluator(SlotId slot_id, TypeDescriptor type) : _slot_id(slot_id), _type(type) {}

    ~ColumnSlotIdEvaluator() override = default;

    Status init() override { return Status::OK(); }

    std::unique_ptr<ColumnEvaluator> clone() const override {
        return std::make_unique<ColumnSlotIdEvaluator>(_slot_id, _type);
    }

    TypeDescriptor type() const override { return _type; }

    StatusOr<ColumnPtr> evaluate(Chunk* chunk) override {
        DCHECK(chunk->is_slot_exist(_slot_id));
        return chunk->get_column_by_slot_id(_slot_id);
    }

private:
    SlotId _slot_id;
    TypeDescriptor _type;
};

} // namespace starrocks
