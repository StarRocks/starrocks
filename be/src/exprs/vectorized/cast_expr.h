// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr.h"
#include "exprs/vectorized/column_ref.h"

namespace starrocks {
namespace vectorized {

class VectorizedCastExprFactory {
public:
    static Expr* from_thrift(const TExprNode& node, bool exception_if_failed = false);

    static Expr* from_type(const TypeDescriptor& from, const TypeDescriptor& to, Expr* child, ObjectPool* pool,
                           bool exception_if_failed = false);
};

// cast Array to Array.
// only support cast the array to another array with the same nested level
// For example.
//   cast Array<int> to Array<String> is OK
//   cast Array<int> to Array<Array<int>> is not OK
class VectorizedCastArrayExpr final : public Expr {
public:
    VectorizedCastArrayExpr(Expr* cast_element_expr, const TExprNode& node)
            : Expr(node), _cast_element_expr(cast_element_expr) {}

    ~VectorizedCastArrayExpr() override = default;

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* ptr) override {
        ColumnPtr column = _children[0]->evaluate(context, ptr);
        if (ColumnHelper::count_nulls(column) == column->size()) {
            return ColumnHelper::create_const_null_column(column->size());
        }
        ColumnPtr cast_column = column->clone_shared();
        ArrayColumn::Ptr array_col = nullptr;
        NullableColumn::Ptr nullable_col = nullptr;
        ColumnPtr src_col = cast_column;

        if (src_col->is_nullable()) {
            nullable_col = (ColumnHelper::as_column<NullableColumn>(src_col));
            src_col = nullable_col->data_column();
        }
        while (src_col->is_array()) {
            array_col = (ColumnHelper::as_column<ArrayColumn>(src_col));
            src_col = array_col->elements_column();
            if (src_col->is_nullable()) {
                nullable_col = (ColumnHelper::as_column<NullableColumn>(src_col));
                src_col = nullable_col->data_column();
            } else {
                nullable_col = nullptr;
            }
        }

        if (nullable_col != nullptr) {
            src_col = nullable_col;
        }
        ChunkPtr chunk = std::make_shared<Chunk>();
        auto column_ref = _cast_element_expr->get_child(0);
        SlotId slot_id = (reinterpret_cast<ColumnRef*>(column_ref))->slot_id();
        chunk->append_column(src_col, slot_id);
        ColumnPtr dest_col = _cast_element_expr->evaluate(nullptr, chunk.get());
        dest_col = ColumnHelper::unfold_const_column(column_ref->type(), chunk->num_rows(), dest_col);

        if (src_col->is_nullable() && !dest_col->is_nullable()) {
            // if the original column is nullable
            auto nullable_col = (ColumnHelper::as_column<NullableColumn>(src_col))->null_column();
            array_col->elements_column() = NullableColumn::create(dest_col, nullable_col);
        } else {
            array_col->elements_column() = dest_col;
        }
        return cast_column;
    };

    Expr* clone(ObjectPool* pool) const override { return pool->add(new VectorizedCastArrayExpr(*this)); }

private:
    Expr* _cast_element_expr;
};

} // namespace vectorized
} // namespace starrocks
