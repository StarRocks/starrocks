// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/array_map_expr.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/vectorized/builtin_functions.h"
#include "exprs/vectorized/function_helper.h"
#include "exprs/vectorized/lambda_function.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {
ArrayMapExpr::ArrayMapExpr(const TExprNode& node) : Expr(node, false) {}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions does not change the offsets of the current array and the null map.
// for many valid arguments:
// if one of them is a null literal, the result is null;
// if one of them is only null, then results are null;
// unfold const columns.
ColumnPtr ArrayMapExpr::evaluate(ExprContext* context, Chunk* ptr) {
    std::vector<ColumnPtr> inputs;

    NullColumnPtr input_null_map = nullptr;
    shared_ptr<ArrayColumn> input_array = nullptr;
    for (int i = 1; i < _children.size(); ++i) {
        ColumnPtr child_col = EVALUATE_NULL_IF_ERROR(context, _children[1], ptr);
        // the column is a null literal.
        if (child_col->is_constant() && child_col->size() == 1 && child_col->is_null(0)) {
            return child_col;
        }
        if (child_col->only_null()) {
            return child_col;
        }
        // no optimization for const columns.
        child_col = ColumnHelper::unpack_and_duplicate_const_column(child_col->size(), child_col);

        auto column = child_col;
        if (child_col->is_nullable()) {
            auto nullable = std::dynamic_pointer_cast<NullableColumn>(child_col);
            DCHECK(nullable != nullptr);
            column = nullable->data_column();
            if (input_null_map) {
                input_null_map =
                        FunctionHelper::union_null_column(nullable->null_column(), input_null_map); // merge null
            } else {
                input_null_map = nullable->null_column();
            }
        }
        DCHECK(column->is_array());
        input_array = std::dynamic_pointer_cast<ArrayColumn>(column);
        if (input_array->elements_column()->size() == 0) { // all elements are null
            return child_col;
        } else {
            inputs.push_back(input_array);
        }
    }

    // construct a new chunk to evaluate the lambda expression.
    auto _cur_chunk = std::make_shared<vectorized::Chunk>();
    // put all arguments into the new chunk
    vector<SlotId> arguments_ids;
    auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
    int argument_num = lambda_func->get_lambda_arguments_ids(&arguments_ids);
    for (int i = 0; i < argument_num; ++i) {
        _cur_chunk->append_column(inputs[i], arguments_ids[i]); // column ref
    }
    // put captured columns into the new chunks aligning with the first array's offsets
    vector<SlotId> slot_ids;
    _children[0]->get_slot_ids(&slot_ids);
    for (auto id : slot_ids) {
        DCHECK(id > 0);
        auto captured = ptr->get_column_by_slot_id(id);
        auto aligned_col = captured->clone_empty();
        _cur_chunk->append_column(
                ColumnHelper::duplicate_column(captured, std::move(aligned_col), input_array->offsets_column()), id);
    }
    auto column = EVALUATE_NULL_IF_ERROR(context, _children[0], _cur_chunk.get());

    // construct the result array
    DCHECK(column != nullptr);
    // the elements of the new array should be nullable and not const.
    column = ColumnHelper::unpack_and_duplicate_const_column(column->size(), column);
    if (auto nullable = std::dynamic_pointer_cast<NullableColumn>(column);
        nullable == nullptr && !column->is_nullable()) {
        NullColumnPtr null_col = NullColumn::create(column->size(), 0);
        column = NullableColumn::create(std::move(column), null_col);
    }
    auto array_col = std::make_shared<ArrayColumn>(column, input_array->offsets_column());
    if (input_null_map != nullptr) {
        return NullableColumn::create(std::move(array_col), input_null_map);
    }
    return array_col;
}

} // namespace starrocks::vectorized
