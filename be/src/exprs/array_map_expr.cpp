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
#include <memory>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/array_view_column.h"
#include "column/vectorized_fwd.h"
#include "common/constexpr.h"
#include "common/statusor.h"
#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/function_helper.h"
#include "exprs/lambda_function.h"
#include "runtime/user_function_cache.h"
#include "storage/chunk_helper.h"

namespace starrocks {
ArrayMapExpr::ArrayMapExpr(const TExprNode& node) : Expr(node, false) {}

ArrayMapExpr::ArrayMapExpr(TypeDescriptor type) : Expr(std::move(type), false) {}

Status ArrayMapExpr::prepare(RuntimeState* state, ExprContext* context) {
    for (int i = 1;i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state, context));
    }
    auto lambda_expr = down_cast<LambdaFunction*>(_children[0]);
    // before prepare lambda
    // collect max slot id
    LambdaFunction::ExtractContext extract_ctx;
    extract_ctx.next_slot_id = lambda_expr->max_used_slot_id() + 1;

    LOG(INFO) << "ArrayMap::prepre, next slot id: " << extract_ctx.next_slot_id;
    RETURN_IF_ERROR(lambda_expr->extract_outer_common_exprs(state, &extract_ctx));
    _outer_common_exprs.swap(extract_ctx.outer_common_exprs);

    for (auto [_, expr]: _outer_common_exprs) {
        RETURN_IF_ERROR(expr->prepare(state, context));
    }
    RETURN_IF_ERROR(lambda_expr->prepare(state, context));

    return Status::OK();
}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions do not change the offsets of the current array and the null map.
// NOTE the return column must be of the return type.
StatusOr<ColumnPtr> ArrayMapExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    std::vector<ColumnPtr> input_elements;
    NullColumnPtr null_column = nullptr;
    bool is_single_nullable_child = false;
    // ArrayColumn* input_array = nullptr;
    ColumnPtr input_array = nullptr;
    ColumnPtr input_array_ptr_ref = nullptr; // hold shared_ptr to avoid early deleted.

    ColumnPtr aligned_offsets;
    // @TODO we should eval common expr first

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
        LOG(INFO) << "eval child: " << child_col->get_name() << ", " << _children[i]->debug_string();
        // no optimization for const columns.
        // if (child_col->is_constant()) {
        //     LOG(INFO) << "unpack const, " << child_col->get_name();
        // }

        bool is_const = child_col->is_constant();
        bool is_nullable = child_col->is_nullable();
        size_t num_rows = child_col->size();

        auto data_column = child_col;
        if (is_const) {
            auto const_column = down_cast<const ConstColumn*>(child_col.get());
            data_column = const_column->data_column();
        }

        // child_col = ColumnHelper::unpack_and_duplicate_const_column(child_col->size(), child_col);
        // @TODO consider const nullable
        if (is_nullable) {
            // auto nullable = down_cast<const NullableColumn*>(child_col.get());
            auto nullable_column = down_cast<const NullableColumn*>(data_column.get());
            DCHECK(nullable_column);
            data_column = nullable_column->data_column();
            // empty null array with non-zero elements
            // @TODO can we remove it??
            data_column->empty_null_in_complex_column(nullable_column->null_column()->get_data(),
                                                 down_cast<const ArrayColumn*>(data_column.get())->offsets().get_data());

            // @TODO what is is_single_nullable_child..
            if (null_column) {
                is_single_nullable_child = false;
                null_column = FunctionHelper::union_null_column(nullable_column->null_column(), null_column); // merge null
            } else {
                is_single_nullable_child = true;
                null_column = nullable_column->null_column();
            }
        }
        DCHECK(data_column->is_array() && !data_column->is_nullable());
        // @TODO column maybe const
        // auto cur_array = down_cast<ArrayColumn*>(column.get());

        // @TODO should keep one column ,make sure array len is same

        auto array_column = down_cast<const ArrayColumn*>(data_column.get());
        // check each array size in this column?
        if (input_array == nullptr) {
            // input_array = cur_array;
            input_array = data_column;
            input_array_ptr_ref = data_column;
            LOG(INFO) << "input_array: " << data_column->get_name();
            // compute aligned_offsets
            if (is_const) {
                aligned_offsets = ColumnHelper::unpack_and_duplicate_const_column(child_col->size(), ConstColumn::create(array_column->offsets_column(), 1));
            } else {
                aligned_offsets = array_column->offsets_column();
            }

        } else {
            // @TODO need a function to check each array size
            // if (UNLIKELY(!ColumnHelper::offsets_equal(cur_array->offsets_column(), input_array->offsets_column()))) {
            //     return Status::InternalError("Input array element's size is not equal in array_map().");
            // }
        }

        // @TODO
        // elements maybe const 
        ColumnPtr elements_column = nullptr;
        if (is_const) {
            // if original column is const column, should keep const
            elements_column = ConstColumn::create(array_column->elements_column(), num_rows);
        } else {
            elements_column = array_column->elements_column();
        }
        
        // @TODO put all element column into input elements
        input_elements.emplace_back(elements_column);

        // input_elements.push_back(cur_array->elements_column());
    }


    if (is_single_nullable_child) {
        DCHECK(null_column != nullptr);
        // If there are more than one nullable children, the nullable column has been cloned when calling
        // union_null_column to merge, so only one nullable child needs to be cloned.
        null_column = ColumnHelper::as_column<NullColumn>(null_column->clone_shared());
    }

    ColumnPtr column = nullptr;
    // @TODO handle empty case?
    // @TODO if all is null
    if (null_column->only_null()) {
        // @TODO need check
    // if (input_array->elements_column()->empty()) { // arrays may be null or empty
        column = ColumnHelper::create_column(type().children[0],
                                             true); // array->elements must be of return array->elements' type
    } else {
        // construct a new chunk to evaluate the lambda expression.
        auto cur_chunk = std::make_shared<Chunk>();
        // @TODO assign column id
        // @TODO eval common expr 

        // put all arguments into the new chunk
        std::vector<SlotId> arguments_ids;
        auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
        int argument_num = lambda_func->get_lambda_arguments_ids(&arguments_ids);
        DCHECK(argument_num == input_elements.size());
        for (int i = 0; i < argument_num; ++i) {
            cur_chunk->append_column(input_elements[i], arguments_ids[i]); // column ref
            LOG(INFO) << "input elements: " << input_elements[i]->get_name() << ", arg id: " << arguments_ids[i];
        }
        // @TODO how to know

        // @TODO we can choos to filter all non before eval?, not sure

        // @TODO capture column dont need 
        // @TODO align all element column
        // put captured columns into the new chunk aligning with the first array's offsets
        // @TODO we don't need align?
        

        // const auto& independent_capture_expr = lambda_func->get_independent_capture_exprs();
        LOG(INFO) << "eval outer common exprs, size: " << _outer_common_exprs.size();
        for (const auto& [column_ref, expr]: _outer_common_exprs) {
            auto slot_id = down_cast<ColumnRef*>(column_ref)->slot_id();
            LOG(INFO) << "eval non-capture expr: " << slot_id;
            ASSIGN_OR_RETURN(auto col, context->evaluate(expr, chunk));
            chunk->append_column(col, slot_id);
        }
        std::vector<SlotId> slot_ids;
        lambda_func->get_slot_ids(&slot_ids);
        for (auto id: slot_ids) {
            LOG(INFO) << "lambda capture column: " << id << ", " << chunk->get_column_by_slot_id(id)->get_name();
        }
        for (auto id : slot_ids) {
            DCHECK(id > 0);
            auto captured = chunk->get_column_by_slot_id(id);
            if (UNLIKELY(captured->size() < input_array->size())) {
                return Status::InternalError(fmt::format(
                        "The size of the captured column {} is less than array's size.", captured->get_name()));
            }
            // @TODO maybe we need binary view column too...
            // @TODO if capture is a binary column, replicate is expansive too
            // @TODO not sure if captured is array??
            LOG(INFO) << "capture column: " << captured->get_name() << ", id: " << id
                << ", size: " << captured->size() << ", num_rows:" << cur_chunk->num_rows();
            // @TODO how to know
            // if (captured->is_array()) {
            //     LOG(INFO) << "build array view column from captured, slot id: " << id;
            //     ASSIGN_OR_RETURN(captured, ArrayViewColumn::from_array_column(captured));
            //     captured->check_or_die();
            // }
            // capture column may be not lambda arguement?
            /// if this capture column is not lambada argument, we treat it as const column to avoid slot
            auto offsets = down_cast<const UInt32Column*>(aligned_offsets.get())->get_data();

            // align offsets
            LOG(INFO) << "relicate captured column, id: "<<id;
            cur_chunk->append_column(captured->replicate(offsets), id);
        }
        
        // @TODO
        {
            // @TODO evalu param may be very large??
            // cut tmp chunk from cur_chunk, and eval
            // cut data
            // if cur_chunk has view_column, we should convert view_column to column again

            // @TODO cut row [x,y] into a tmp chunk
            ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
            RETURN_IF_ERROR(accumulator.push(std::move(cur_chunk)));
            accumulator.finalize();
            while (auto tmp_chunk = accumulator.pull()) {
                // if contains view, should translate it back
                // TODO change column 
                auto new_chunk = std::make_shared<Chunk>();
                const auto& columns = tmp_chunk->columns();
                for(size_t idx = 0;idx < columns.size();idx++) {
                    const auto& column = columns[idx];
                    if (column->is_array_view()) {
                        LOG(INFO) << "convert array-view to array, " << column->get_name();
                        ASSIGN_OR_RETURN(auto new_column, ArrayViewColumn::to_array_column(column));
                        LOG(INFO) << "convert done";
                        new_column->check_or_die();
                        // auto array_view_column = down_cast<const ArrayViewColumn*>(column.get());
                        // ASSIGN_OR_RETURN(auto new_column, array_view_column->to_array_column());
                        LOG(INFO) << "update column, idx: " << idx;
                        tmp_chunk->update_column_by_index(new_column, idx);
                    }
                }
                tmp_chunk->check_or_die();
                for (const auto& column: tmp_chunk->columns()) {
                    LOG(INFO) << "column: " << column->get_name();
                    DCHECK(!column->is_array_view()) << "unexpected array view";
                }

                ASSIGN_OR_RETURN(auto tmp_col, context->evaluate(_children[0], tmp_chunk.get()));
                tmp_col = ColumnHelper::align_return_type(tmp_col, type().children[0], tmp_chunk->num_rows(), true);
                if (column == nullptr) {
                    column = tmp_col;
                } else {
                    column->append(*tmp_col);
                }
            }
        }


        // @TODO
        // evaluate lambda expr?
        // if (cur_chunk->num_rows() <= chunk->num_rows() * 8) {
        //     ASSIGN_OR_RETURN(column, context->evaluate(_children[0], cur_chunk.get()));
        //     column = ColumnHelper::align_return_type(column, type().children[0], cur_chunk->num_rows(), true);
        // } else { // split large chunks into small ones to avoid too large or various batch_size
        //     ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
        //     RETURN_IF_ERROR(accumulator.push(std::move(cur_chunk)));
        //     accumulator.finalize();
        //     while (auto tmp_chunk = accumulator.pull()) {
        //         ASSIGN_OR_RETURN(auto tmp_col, context->evaluate(_children[0], tmp_chunk.get()));
        //         tmp_col = ColumnHelper::align_return_type(tmp_col, type().children[0], tmp_chunk->num_rows(), true);
        //         if (column == nullptr) {
        //             column = tmp_col;
        //         } else {
        //             column->append(*tmp_col);
        //         }
        //     }
        // }

        // construct the result array
        DCHECK(column != nullptr);
        column = ColumnHelper::cast_to_nullable_column(column);
    
    }
    // @TODO handle const?

    // attach offsets
    auto array_col = std::make_shared<ArrayColumn>(
            column, ColumnHelper::as_column<UInt32Column>(aligned_offsets->clone_shared()));

    if (null_column != nullptr) {
        return NullableColumn::create(std::move(array_col), null_column);
    }
    return array_col;
}

} // namespace starrocks
