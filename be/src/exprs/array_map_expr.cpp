// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/array_map_expr.h"

#include <fmt/format.h>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/function_helper.h"
#include "exprs/lambda_function.h"
#include "runtime/user_function_cache.h"
#include "storage/chunk_helper.h"

namespace starrocks {
ArrayMapExpr::ArrayMapExpr(const TExprNode& node) : Expr(node, false) {}

ArrayMapExpr::ArrayMapExpr(TypeDescriptor type) : Expr(std::move(type), false) {}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions do not change the offsets of the current array and the null map.
// NOTE the return column must be of the return type.
StatusOr<ColumnPtr> ArrayMapExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    std::vector<ColumnPtr> input_elements;
    NullColumnPtr input_null_map = nullptr;
    ArrayColumn* input_array = nullptr;
    ColumnPtr input_array_ptr_ref = nullptr; // hold shared_ptr to avoid early deleted.
    // for many valid arguments:
    // if one of them is a null literal, the result is a null literal;
    // if one of them is only null, then results are null;
    // unfold const columns.
    // make sure all inputs have the same offsets.
    // TODO(fzh): support several arrays with different offsets and set null for non-equal size of arrays.
    for (int i = 1; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(auto child_col, context->evaluate(_children[i], chunk));
        // the column is a null literal.
        if (child_col->only_null()) {
            return ColumnHelper::align_return_type(child_col, type(), chunk->num_rows(), true);
        }
        // no optimization for const columns.
        child_col = ColumnHelper::unpack_and_duplicate_const_column(child_col->size(), child_col);

        auto column = child_col;
        if (child_col->is_nullable()) {
            auto nullable = down_cast<const NullableColumn*>(child_col.get());
            DCHECK(nullable != nullptr);
            column = nullable->data_column();
            // empty null array with non-zero elements
            column->empty_null_in_complex_column(nullable->null_column()->get_data(),
                                                 down_cast<const ArrayColumn*>(column.get())->offsets().get_data());
            if (input_null_map) {
                input_null_map =
                        FunctionHelper::union_null_column(nullable->null_column(), input_null_map); // merge null
            } else {
                input_null_map = nullable->null_column();
            }
        }
        DCHECK(column->is_array());
        auto cur_array = down_cast<ArrayColumn*>(column.get());

        if (input_array == nullptr) {
            input_array = cur_array;
            input_array_ptr_ref = column;
        } else {
            if (UNLIKELY(!ColumnHelper::offsets_equal(cur_array->offsets_column(), input_array->offsets_column()))) {
                return Status::InternalError("Input array element's size is not equal in array_map().");
            }
        }
        input_elements.push_back(cur_array->elements_column());
    }

    ColumnPtr column = nullptr;
    if (input_array->elements_column()->empty()) { // arrays may be null or empty
        column = ColumnHelper::create_column(type().children[0],
                                             true); // array->elements must be of return array->elements' type
    } else {
        // construct a new chunk to evaluate the lambda expression.
        auto cur_chunk = std::make_shared<Chunk>();
        // put all arguments into the new chunk
        std::vector<SlotId> arguments_ids;
        auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
        int argument_num = lambda_func->get_lambda_arguments_ids(&arguments_ids);
        DCHECK(argument_num == input_elements.size());
        for (int i = 0; i < argument_num; ++i) {
            cur_chunk->append_column(input_elements[i], arguments_ids[i]); // column ref
        }
        // put captured columns into the new chunk aligning with the first array's offsets
        std::vector<SlotId> slot_ids;
        _children[0]->get_slot_ids(&slot_ids);
        for (auto id : slot_ids) {
            DCHECK(id > 0);
            auto captured = chunk->get_column_by_slot_id(id);
            if (UNLIKELY(captured->size() < input_array->size())) {
                return Status::InternalError(fmt::format(
                        "The size of the captured column {} is less than array's size.", captured->get_name()));
            }
            cur_chunk->append_column(captured->replicate(input_array->offsets_column()->get_data()), id);
        }
        if (cur_chunk->num_rows() <= chunk->num_rows() * 8) {
            ASSIGN_OR_RETURN(column, context->evaluate(_children[0], cur_chunk.get()));
            column = ColumnHelper::align_return_type(column, type().children[0], cur_chunk->num_rows(), true);
        } else { // split large chunks into small ones to avoid too large or various batch_size
            ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
            RETURN_IF_ERROR(accumulator.push(std::move(cur_chunk)));
            accumulator.finalize();
            while (auto tmp_chunk = accumulator.pull()) {
                ASSIGN_OR_RETURN(auto tmp_col, context->evaluate(_children[0], tmp_chunk.get()));
                tmp_col = ColumnHelper::align_return_type(tmp_col, type().children[0], tmp_chunk->num_rows(), true);
                if (column == nullptr) {
                    column = tmp_col;
                } else {
                    column->append(*tmp_col);
                }
            }
        }

        // construct the result array
        DCHECK(column != nullptr);
        column = ColumnHelper::cast_to_nullable_column(column);
    }
    // attach offsets
    auto array_col = std::make_shared<ArrayColumn>(column, input_array->offsets_column());
    if (input_null_map != nullptr) {
        return NullableColumn::create(std::move(array_col), input_null_map);
    }
    return array_col;
}

} // namespace starrocks
