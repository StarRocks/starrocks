// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once


#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/hash_set.h"
#include "common/object_pool.h"
#include "exprs/predicate.h"

namespace starrocks {
namespace vectorized {

template <PrimitiveType Type>
class VectorizedInIteratorPredicate final : public Predicate {
public:
    VectorizedInIteratorPredicate(const TExprNode& node)
            : Predicate(node), _is_not_in(node.in_predicate.is_not_in) {}

    VectorizedInIteratorPredicate(const VectorizedInIteratorPredicate& other)
            : Predicate(other), _is_not_in(other._is_not_in) {}

    ~VectorizedInIteratorPredicate() override = default;

    Expr* clone(ObjectPool* pool) const override {
        return pool->add(new VectorizedInIteratorPredicate(*this));
    }

    Status open(RuntimeState* state, ExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        RETURN_IF_ERROR(Expr::open(state, context, scope));

        if (Type != _children[0]->type().type) {
            if (!isSlicePT<Type> || !_children[0]->type().is_string_type()) {
                return Status::InternalError("VectorizedInPredicate type is error");
            }
        }

        for (int i = 1; i < _children.size(); ++i) {
            if ((_children[0]->type().is_string_type() && _children[i]->type().is_string_type()) ||
                (_children[0]->type().type == _children[i]->type().type) ||
                (PrimitiveType::TYPE_NULL == _children[i]->type().type)) {
                // pass
            } else {
                return Status::InternalError("VectorizedInPredicate type not same");
            }
        }

        _hit_value = (_is_not_in == 0);
        _not_hit_value = (_is_not_in != 0);

        return Status::OK();
    }

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* chunk) override {
        auto find = _children[0]->evaluate(context, chunk);

        if (ColumnHelper::count_nulls(find) == find->size()) {
            return ColumnHelper::create_const_null_column(find->size());
        }

        std::vector<ColumnViewer<Type>> value_viewers;
        value_viewers.reserve(_children.size());

        Columns columns;
        columns.reserve(_children.size());
        columns.emplace_back(find);

        bool has_all_null = false;
        for (int i = 1; i < _children.size(); ++i) {
            auto value = _children[i]->evaluate(context, chunk);

            if (ColumnHelper::count_nulls(value) == value->size()) {
                has_all_null = true;
                continue;
            }

            columns.emplace_back(value);
            value_viewers.emplace_back(ColumnViewer<Type>(value));
        }

        ColumnViewer<Type> find_viewer(find);
        ColumnBuilder<TYPE_BOOLEAN> builder;

        size_t size = columns[0]->size();
        for (int row = 0; row < size; ++row) {
            if (find_viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            auto fv = find_viewer.value(row);
            bool has_null = has_all_null;
            bool found = false;

            for (const auto& viewer : value_viewers) {
                if (viewer.is_null(row)) {
                    has_null = true;
                } else if (fv == viewer.value(row)) {
                    found = true;
                    builder.append(_hit_value);
                    break;
                }
            }

            if (found) {
                continue;
            } else if (has_null) {
                builder.append_null();
                continue;
            }

            builder.append(_not_hit_value);
        }

        return builder.build(ColumnHelper::is_all_const(columns));
    }

private:
    const bool _is_not_in;

    bool _hit_value = true;
    bool _not_hit_value = false;
};
} // namespace vectorized
} // namespace starrocks
