// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

inline bool offsets_equal(UInt32Column::Ptr array1, UInt32Column::Ptr array2) {
    if (array1->size() != array2->size()) {
        return false;
    }
    auto data1 = array1->get_data();
    auto data2 = array2->get_data();
    return std::equal(data1.begin(), data1.end(), data2.begin());
}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions does not change the offsets of the current array and the null map.
ColumnPtr ArrayMapExpr::evaluate(ExprContext* context, Chunk* chunk) {
    std::vector<ColumnPtr> inputs;
    NullColumnPtr input_null_map = nullptr;
    std::shared_ptr<ArrayColumn> input_array = nullptr;
    // for many valid arguments:
    // if one of them is a null literal, the result is a null literal;
    // if one of them is only null, then results are null;
    // unfold const columns.
    // make sure all inputs have the same offsets.
    // TODO(fzh): support several arrays with different offsets and set null for non-equal size of arrays.
    for (int i = 1; i < _children.size(); ++i) {
        ColumnPtr child_col = EVALUATE_NULL_IF_ERROR(context, _children[i], chunk);
        // the column is a null literal.
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
            // empty null array with non-zero elements
            std::dynamic_pointer_cast<ArrayColumn>(column)->empty_null_array(nullable->null_column());
            if (input_null_map) {
                input_null_map =
                        FunctionHelper::union_null_column(nullable->null_column(), input_null_map); // merge null
            } else {
                input_null_map = nullable->null_column();
            }
        }
        DCHECK(column->is_array());
        auto cur_array = std::dynamic_pointer_cast<ArrayColumn>(column);

        if (input_array == nullptr) {
            input_array = cur_array;
        } else {
            if (!offsets_equal(cur_array->offsets_column(), input_array->offsets_column())) {
                throw std::runtime_error("Input array element's size is not equal in array_map().");
            }
        }
        if (cur_array->elements_column()->size() == 0) { // all elements are null
            return child_col;
        } else {
            inputs.push_back(cur_array->elements_column());
        }
    }

    // construct a new chunk to evaluate the lambda expression.
    auto cur_chunk = std::make_shared<vectorized::Chunk>();
    // put all arguments into the new chunk
    vector<SlotId> arguments_ids;
    auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
    int argument_num = lambda_func->get_lambda_arguments_ids(&arguments_ids);
    DCHECK(argument_num == inputs.size());
    for (int i = 0; i < argument_num; ++i) {
        cur_chunk->append_column(inputs[i], arguments_ids[i]); // column ref
    }
    // put captured columns into the new chunks aligning with the first array's offsets
    vector<SlotId> slot_ids;
    _children[0]->get_slot_ids(&slot_ids);
    for (auto id : slot_ids) {
        DCHECK(id > 0);
        auto captured = chunk->get_column_by_slot_id(id);
        auto aligned_col = captured->clone_empty();
        cur_chunk->append_column(
                ColumnHelper::duplicate_column(captured, std::move(aligned_col), input_array->offsets_column()), id);
    }
    auto column = EVALUATE_NULL_IF_ERROR(context, _children[0], cur_chunk.get());

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
