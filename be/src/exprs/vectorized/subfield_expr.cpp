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
    explicit SubfieldExpr(const TExprNode& node) : Expr(node), _used_subfield_names(node.used_subfield_names) {}

    SubfieldExpr(const SubfieldExpr&) = default;
    SubfieldExpr(SubfieldExpr&&) = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* chunk) override {
        DCHECK_EQ(1, _children.size());

        ASSIGN_OR_RETURN(ColumnPtr col, _children.at(0)->evaluate_checked(context, chunk));

        // Enter multiple subfield for struct type, remain last subfield
        for (size_t i = 0; i < _used_subfield_names.size() - 1; i++) {
            std::string fieldname = _used_subfield_names[i];
            Column* tmp_col = ColumnHelper::get_data_column(col.get());
            DCHECK(tmp_col->is_struct());
            auto* struct_column = down_cast<StructColumn*>(tmp_col);
            col = struct_column->field_column(fieldname);
            if (col == nullptr) {
                LOG(WARNING) << "Struct subfield name: " + fieldname + " not found!";
                // Return an empty column to avoid nullptr
                return ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_NULL), true);
            }
        }

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
        auto* struct_column = down_cast<StructColumn*>(tmp_col);

        std::string fieldname = _used_subfield_names.back();
        ColumnPtr subfield_column = struct_column->field_column(fieldname);
        if (subfield_column == nullptr) {
            LOG(WARNING) << "Struct subfield name: " + fieldname + " not found!";
            // Return an empty column to avoid nullptr
            return ColumnHelper::create_column(TypeDescriptor::from_primtive_type(PrimitiveType::TYPE_NULL), true);
        }
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

        // We need clone a new subfield column
        return NullableColumn::create(subfield_column->clone_shared(), result_null);
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new SubfieldExpr(*this)); }

private:
    std::vector<std::string> _used_subfield_names;
};

Expr* SubfieldExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::SUBFIELD_EXPR, node.node_type);
    DCHECK(node.__isset.used_subfield_names);
    return new SubfieldExpr(node);
}

} // namespace starrocks::vectorized