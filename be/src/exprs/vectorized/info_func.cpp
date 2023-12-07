// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/info_func.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"

namespace starrocks::vectorized {

VectorizedInfoFunc::VectorizedInfoFunc(const TExprNode& node) : Expr(node) {
    switch (_type.type) {
    case TYPE_BIGINT: {
        _value = ColumnHelper::create_const_column<TYPE_BIGINT>(node.info_func.int_value, 1);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        // @IMPORTANT: build slice though get_data, else maybe will case multi-thread crash in scanner
        _value = ColumnHelper::create_const_column<TYPE_VARCHAR>(node.info_func.str_value, 1);
        break;
    }
    default:
        DCHECK(false) << "Vectorized engine not implement type: " << _type.type;
        break;
    }
}

StatusOr<ColumnPtr> VectorizedInfoFunc::evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) {
    ColumnPtr column = _value->clone_empty();
    column->append(*_value, 0, 1);
    if (ptr != nullptr) {
        column->resize(ptr->num_rows());
    }
    return column;
}

std::string VectorizedInfoFunc::debug_string() const {
    std::stringstream out;
    out << "VectorizedInfoFunc("
        << "type=" << this->type().debug_string() << " )";
    return out.str();
}

VectorizedInfoFunc::~VectorizedInfoFunc() = default;

} // namespace starrocks::vectorized
