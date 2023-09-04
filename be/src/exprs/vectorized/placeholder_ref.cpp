// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/placeholder_ref.h"

#include "column/chunk.h"

namespace starrocks::vectorized {
PlaceHolderRef::PlaceHolderRef(const TExprNode& node) : Expr(node, true), _column_id(node.vslot_ref.slot_id) {}

StatusOr<ColumnPtr> PlaceHolderRef::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ColumnPtr& column = (ptr)->get_column_by_slot_id(_column_id);
    return column;
}

} // namespace starrocks::vectorized
