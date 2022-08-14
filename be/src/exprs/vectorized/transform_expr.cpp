// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/transform_expr.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/builtin_functions.h"
#include "exprs/vectorized/lambda_function.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {
TransformExpr::TransformExpr(const TExprNode& node) : Expr(node, false) {}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions does not change the offsets of the current array and the null map.
ColumnPtr TransformExpr::evaluate(ExprContext* context, Chunk* ptr) {
    ColumnPtr child_col = EVALUATE_NULL_IF_ERROR(context, _children[0], ptr);
    NullColumnPtr null_map = nullptr;
    ColumnPtr column = child_col;
    if (auto nullable = std::dynamic_pointer_cast<NullableColumn>(child_col); nullable != nullptr) {
        column = nullable->data_column();
        null_map = nullable->null_column();
    }
    auto array_col = std::dynamic_pointer_cast<ArrayColumn>(column);
    if (child_col->only_null() || array_col->elements_column()->size() == 0) { // all elements are null
        column = ColumnHelper::create_column(_children[1]->get_child(1)->type(), _children[1]->is_nullable());
    } else {
        // construct a new chunk to evaluate the lambda expression.
        auto _cur_chunk = std::make_shared<vectorized::Chunk>();
        // put all arguments into the new chunk, only 1 at present.
        vector<SlotId> arguments_ids;
        auto lambda_func = dynamic_cast<LambdaFunction*>(_children[1]);
        int argument_num = lambda_func->get_lambda_arguments_ids(&arguments_ids);
        for (int i = 0; i < argument_num; ++i) {
            _cur_chunk->append_column(array_col->elements_column(), arguments_ids[i]); // column ref
        }
        // put captured columns into the new chunks aligning with the first array's offsets
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
    }
    DCHECK(column != nullptr);
    // the elements of the new array should be nullable and not const.
    column = ColumnHelper::unpack_and_duplicate_const_column(column->size(), column);
    if (auto nullable = std::dynamic_pointer_cast<NullableColumn>(column);
        nullable == nullptr && !column->is_nullable()) {
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