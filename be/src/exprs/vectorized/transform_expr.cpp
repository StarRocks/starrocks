// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/transform_expr.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "column/array_column.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/builtin_functions.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {
TransformExpr::TransformExpr(const TExprNode& node) : Expr(node, false) {}


// The input column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions does not change the offsets of the current array and the null map.
ColumnPtr TransformExpr::evaluate(ExprContext* context, Chunk* ptr) {
    ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, _children[0], ptr);
    // take the result column (elements of array) to construct a chunk
    auto _cur_chunk = std::make_shared<vectorized::Chunk>();
    NullColumnPtr null_map = nullptr;
    if (auto nullable = std::dynamic_pointer_cast<NullableColumn>(column); nullable != nullptr) {
        column = nullable->data_column();
        null_map = nullable->null_column();
    }
    auto array_col = std::dynamic_pointer_cast<ArrayColumn>(column);
    _cur_chunk->append_column(array_col->elements_column(), 111); // column ref
    vector<SlotId> slot_ids;
    _children[1]->get_slot_ids(&slot_ids);
    for (auto id : slot_ids) {
        DCHECK(id > 0);
        auto captured = ptr->get_column_by_slot_id(id);
        auto aligned_col = captured->clone_empty();
        _cur_chunk->append_column(
                ColumnHelper::duplicate_column(captured, std::move(aligned_col), array_col->offsets_column()), id);
    }
    column = EVALUATE_NULL_IF_ERROR(context, _children[1], _cur_chunk.get());
    DCHECK(column != nullptr);
    // the elements of arrays should be nullable, TODO, but the column is not nullable, a conflict comes here.
    if (auto nullable = std::dynamic_pointer_cast<NullableColumn>(column); nullable == nullptr && column->is_nullable()) {
        NullColumnPtr null_col = NullColumn::create(column->size(), 0);
        column = NullableColumn::create(std::move(column), null_col);
    }
    array_col = std::make_shared<ArrayColumn>(column, array_col->offsets_column());
    if (null_map != nullptr) {
        return NullableColumn::create(std::move(array_col), null_map);
    }
    return array_col;
}

} // namespace starrocks::vectorized