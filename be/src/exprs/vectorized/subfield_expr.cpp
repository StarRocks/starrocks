// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Inc.

#include "exprs/vectorized/subfield_expr.h"

#include "column/column_helper.h"
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
        Column* tmp_col = ColumnHelper::get_data_column(col.get());
        DCHECK(tmp_col->is_struct());
        StructColumn* struct_column = down_cast<StructColumn*>(tmp_col);
        return struct_column->field_column(_used_subfield_name);
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