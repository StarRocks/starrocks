// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/subfield_expr.h"

#include <cstring>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"

namespace starrocks::vectorized {

class SubfieldExpr final : public Expr {
public:
    explicit SubfieldExpr(const TExprNode& node) : Expr(node), _used_subfield_name(node.used_subfield_name) {}

    SubfieldExpr(const SubfieldExpr&) = default;
    SubfieldExpr(SubfieldExpr&&) = default;

    ColumnPtr evaluate(ExprContext* context, vectorized::Chunk* chunk) override {
        DCHECK_EQ(1, _children.size());

        ColumnPtr col = _children.at(0)->evaluate(context, chunk);

        // handle nullable column
        std::vector<uint8_t> null_flags;
        size_t num_rows = col->size();
        null_flags.resize(num_rows, false);
        if (col->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(col.get());
            const uint8_t* nulls = nullable->null_column()->raw_data();
            // for (size_t i = 0; i < num_rows; i++) {
            //     null_flags[i] = nulls[i];
            // }
            std::memcpy(&null_flags[0], &nulls[0], num_rows * sizeof(uint8_t));
        }

        Column* tmp_col = ColumnHelper::get_data_column(col.get());
        DCHECK(tmp_col->is_struct());
        StructColumn* struct_column = down_cast<StructColumn*>(tmp_col);

        ColumnPtr subfield_column = struct_column->field_column(_used_subfield_name);
        if (subfield_column->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(subfield_column.get());
            const uint8_t* nulls = nullable->null_column()->raw_data();
            for (size_t i = 0; i < num_rows; i++) {
                null_flags[i] |= nulls[i];
            }
            subfield_column = nullable->data_column();
        }

        NullColumnPtr result_null = NullColumn::create();
        result_null->get_data().swap(null_flags);
        DCHECK_EQ(subfield_column->size(), result_null->size());

        // If you want to edit subfield column, you need clone it first.
        return NullableColumn::create(subfield_column, result_null);
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new SubfieldExpr(*this)); }

private:
    std::string _used_subfield_name;
};

Expr* SubfieldExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::SUBFIELD_EXPR, node.node_type);
    DCHECK(node.__isset.used_subfield_name);
    return new SubfieldExpr(node);
}

} // namespace starrocks::vectorized