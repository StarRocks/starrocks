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
#include "simd/simd.h"
#include "storage/chunk_helper.h"

namespace starrocks {
ArrayMapExpr::ArrayMapExpr(const TExprNode& node) : Expr(node, false) {}

ArrayMapExpr::ArrayMapExpr(TypeDescriptor type) : Expr(std::move(type), false) {}

Status ArrayMapExpr::prepare(RuntimeState* state, ExprContext* context) {
    for (int i = 1;i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state, context));
    }
    // if child 0 is not lambda, what will happen whe nevaluate

    // @TODO if children[0] not lambda
    // @TODO _children[0] maybe not a lambda function?
    auto lambda_expr = down_cast<LambdaFunction*>(_children[0]);
    // before prepare lambda
    // collect max slot id
    LambdaFunction::ExtractContext extract_ctx;
    extract_ctx.next_slot_id = lambda_expr->max_used_slot_id() + 1;

    LOG(INFO) << "ArrayMap::prepare, next slot id: " << extract_ctx.next_slot_id << ", this: " << (void*)this;
    RETURN_IF_ERROR(lambda_expr->extract_outer_common_exprs(state, &extract_ctx));
    _outer_common_exprs.swap(extract_ctx.outer_common_exprs);

    for (auto [_, expr]: _outer_common_exprs) {
        // @TODO 
        LOG(INFO) << "prepare common expr: " << expr->debug_string();
        // @TODO if after rewrite, first expr of array_map become column ref, we can remove it?
        RETURN_IF_ERROR(expr->prepare(state, context));
    }
    RETURN_IF_ERROR(lambda_expr->prepare(state, context));

    return Status::OK();
}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions do not change the offsets of the current array and the null map.
// NOTE the return column must be of the return type.
StatusOr<ColumnPtr> ArrayMapExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    // @TODO just use one vector store array column
    std::vector<ColumnPtr> input_elements;

    // NullColumnPtr null_column = nullptr;
    bool is_single_nullable_child = false;
    // ArrayColumn* input_array = nullptr;
    ColumnPtr input_array = nullptr;
    ColumnPtr input_array_ptr_ref = nullptr; // hold shared_ptr to avoid early deleted.

    // ColumnPtr aligned_offsets;
    UInt32Column::Ptr aligned_offsets;
    // @TODO we should eval common expr first

    // maybe a NullColumn or a Const(NullColumn)
    NullColumnPtr result_null_column = nullptr;
    bool all_input_is_constant = true;
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
        LOG(INFO) << "eval child: " << child_col->get_name();

        bool is_const = child_col->is_constant();
        bool is_nullable = child_col->is_nullable();
        size_t num_rows = child_col->size();
        all_input_is_constant &= is_const;

        auto data_column = child_col;
        if (is_const) {
            auto const_column = down_cast<const ConstColumn*>(child_col.get());
            data_column = const_column->data_column();
        }

        // @TODO consider const nullable
        if (is_nullable) {
            auto nullable_column = down_cast<const NullableColumn*>(data_column.get());
            DCHECK(nullable_column);
            data_column = nullable_column->data_column();
            // empty null array with non-zero elements
            // @TODO can we remove it??

            // @TODO we can check it before??
            // this will re-build data column, replace null element to empty array
            data_column->empty_null_in_complex_column(nullable_column->null_column()->get_data(),
                                                 down_cast<const ArrayColumn*>(data_column.get())->offsets().get_data());

            auto null_column = nullable_column->null_column();
            if (is_const) {
                LOG(INFO) << "input is const, should unpack null column";
                // if null_column is from const_column, should unpack
                null_column->assign(num_rows, 0);
            }

            // try to merge null column
            if (result_null_column) {
                is_single_nullable_child = false;
                // union two null column
                LOG(INFO) << "union result_null_column, size: " << result_null_column->size() << ", null size: " << null_column->size();
                result_null_column = FunctionHelper::union_null_column(null_column, result_null_column);
                LOG(INFO) << "union done: " << result_null_column->size();
            } else {
                is_single_nullable_child = true;
                result_null_column = null_column;
                LOG(INFO) << "assign result_null_column, size: " << null_column->size();
            }
        }
        DCHECK(data_column->is_array() && !data_column->is_nullable());

        ColumnPtr column = data_column;
        if (is_const) {
            // keep it as a Const(ArrayColumn) in input elelents
            column = ConstColumn::create(data_column, num_rows);
        }

        // check each array's lengths in input_elements
        if (!input_elements.empty()) {
            const auto& first_input = input_elements[0];

            bool is_array_lengths_valid = result_null_column ?
                ArrayColumn::is_all_array_lengths_equal<false>(first_input, column, result_null_column):
                ArrayColumn::is_all_array_lengths_equal<true>(first_input, column, result_null_column);
            if (!is_array_lengths_valid) {
                return Status::InternalError("Input array element's size is not equal in array_map().");
            }
        }
        
        input_elements.emplace_back(column);
    }

    if (is_single_nullable_child) {
        DCHECK(result_null_column != nullptr);
        // If there are more than one nullable children, the nullable column has been cloned when calling
        // union_null_column to merge, so only one nullable child needs to be cloned.
        result_null_column = ColumnHelper::as_column<NullColumn>(result_null_column->clone_shared());
    }

    ColumnPtr column = nullptr;
    size_t null_rows = result_null_column ? SIMD::count_nonzero(result_null_column->get_data()): 0;
    if (null_rows == input_elements[0]->size()) {
        // all input is null
        column = ColumnHelper::create_column(type().children[0],
                                             true); // array->elements must be of return array->elements' type
    } else {
        // construct a new chunk to evaluate the lambda expression.
        auto cur_chunk = std::make_shared<Chunk>();

        // 1. evaluate all outer common exprs
        LOG(INFO) << "eval outer common exprs, size: " << _outer_common_exprs.size();
        for (const auto& [column_ref, expr]: _outer_common_exprs) {
            auto slot_id = down_cast<ColumnRef*>(column_ref)->slot_id();
            LOG(INFO) << "eval non-capture expr: " << slot_id;
            ASSIGN_OR_RETURN(auto col, context->evaluate(expr, chunk));
            LOG(INFO) << "col size: " << col->size();
            chunk->append_column(col, slot_id);
        }

        auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
        std::vector<SlotId> slot_ids;
        lambda_func->get_slot_ids(&slot_ids);
        // 2. check captured columns size
        for (auto slot_id : slot_ids) {
            LOG(INFO) << "check slot id: " << slot_id;
            DCHECK(slot_id > 0);
            auto captured_column = chunk->get_column_by_slot_id(slot_id);
            if (UNLIKELY(captured_column->size() < input_elements[0]->size())) {
                return Status::InternalError(fmt::format(
                        "The size of the captured column {} is less than array's size.", captured_column->get_name()));
            }
        }

        // 3. align up all columns offsets
        // if most value is null, we remove all null column, create a new one to evaluate
        // else alignup offset
        // @TODO we can't avoid copy data here??
        // should we replicate capture column???
        // empty all null is ok

        // @TODO if all input is const, we don't need unpack const
        if (all_input_is_constant) {
            // if all input arguments are ConstColumn, we don't need unpack, just evaluate on ConstColumn
            LOG(INFO) << "all inputs of array_map are ConstColumn";

        }
        // @TODO udpate aligned_offsets, we can use arg0's offsets?

        std::vector<SlotId> arguments_ids;
        int argument_num = lambda_func->get_lambda_arguments_ids(&arguments_ids);
        DCHECK(argument_num == input_elements.size());
        for (int i = 0; i < argument_num; ++i) {
            auto data_column = FunctionHelper::get_data_column_of_const(input_elements[i]);
            auto array_column = down_cast<const ArrayColumn*>(data_column.get());
            auto elements_column = array_column->elements_column();
            UInt32Column::Ptr offsets_column = array_column->offsets_column();

            if (input_elements[i]->is_constant()) {
                // if input is const, we should assign data multiple times
                // seems we cant avoid copy data if we don't have view column?
                // if input is const, we should wrap its element column as a const column too
                // @TODO elements should not be a const column
                size_t elements_num = array_column->get_element_size(0);
                elements_column = elements_column->clone();
                // create a new offsets
                // offsets_column = UInt32Column::create();
                offsets_column = UInt32Column::create();
                // replicate N time and ignore null
                size_t repeat_times = input_elements[i]->size() - null_rows;
                offsets_column->append(0);
                size_t offset = elements_num;
                for (size_t i = 0;i < repeat_times;i++) {
                    elements_column->append(*elements_column, 0, elements_num);
                    offset += elements_num;
                    offsets_column->append(offset);
                }

            } else {
                // @TODO null data size is ok, only one row, why offsets has too many data?

                // @TODO empty_null should apply on array column..
                // elements_column->empty_null_in_complex_column(result_null_column->get_data(), array_column->offsets().get_data());
                data_column->empty_null_in_complex_column(result_null_column->get_data(), array_column->offsets().get_data());
                elements_column = down_cast<const ArrayColumn*>(data_column.get())->elements_column();
            }
            if (aligned_offsets == nullptr) {
                aligned_offsets = offsets_column;
            }
            //append elemt
            cur_chunk->append_column(elements_column, arguments_ids[i]);
            LOG(INFO) << "input elements: " << input_elements[i]->get_name() << ", arg id: " << arguments_ids[i];
        }
        // @TODO put outer common expr into cur_chunk,
        // align offset
        for (const auto& [column_ref, expr]: _outer_common_exprs) {
            auto slot_id = down_cast<ColumnRef*>(column_ref)->slot_id();
            auto column = chunk->get_column_by_slot_id(slot_id);
            column = ColumnHelper::unpack_and_duplicate_const_column(column->size(), column);
            // replicate column and put int into cur_chunk
            // @TODO what if column is const?
            // @TODO this should be in cur_chunk and chunk?
            auto aligned_column = column->replicate(aligned_offsets->get_data());
            cur_chunk->append_column(aligned_column, slot_id);
            LOG(INFO) << "append outer common column: " << slot_id;
            // chunk->append_column(col, slot_id);
        }
        for (auto slot_id : slot_ids) {
            DCHECK(slot_id > 0);
            if (cur_chunk->is_slot_exist(slot_id)) {
                continue;
            }
            auto captured_column = chunk->get_column_by_slot_id(slot_id);
            auto aligned_column = captured_column->replicate(aligned_offsets->get_data());
            cur_chunk->append_column(aligned_column, slot_id);
            LOG(INFO) << "append capture column, " << slot_id;
        }
        #ifdef DEBUG
        {
            auto first_column = cur_chunk->get_column_by_slot_id(arguments_ids[0]);
            for (int i = 1;i < argument_num;i++) {
                auto column = cur_chunk->get_column_by_slot_id(arguments_ids[i]);
                DCHECK_EQ(column->size(), first_column->size()) << "input arguments size should be same";
            }
        }
        #endif
        
        // @TODO
        {
            // @TODO evalu param may be very large??
            // cut tmp chunk from cur_chunk, and eval
            // cut data
            // if cur_chunk has view_column, we should convert view_column to column again

            // @TODO can we find common expr from chunk?
            for (const auto& [slot_id, _]: chunk->get_slot_id_to_index_map()) {
                LOG(INFO) << "chunk contains slot id: " << slot_id;
            }
            for (const auto& [slot_id, _] : cur_chunk->get_slot_id_to_index_map()) {
                LOG(INFO) << "cur_chunk contains slot id: " << slot_id;
            }
            // @TODO cut row [x,y] into a tmp chunk
            ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
            LOG(INFO) << "cur_chunk rows: " << cur_chunk->num_rows();
            RETURN_IF_ERROR(accumulator.push(std::move(cur_chunk)));
            accumulator.finalize();
            while (auto tmp_chunk = accumulator.pull()) {
                // if contains view, should translate it back
                // TODO change column 
                auto new_chunk = std::make_shared<Chunk>();
                // const auto& columns = tmp_chunk->columns();
                LOG(INFO) << "tmp_chunk rows: " << tmp_chunk->num_rows();
                // for(size_t idx = 0;idx < columns.size();idx++) {
                //     const auto& column = columns[idx];
                //     if (column->is_array_view()) {
                //         LOG(INFO) << "convert array-view to array, " << column->get_name();
                //         ASSIGN_OR_RETURN(auto new_column, ArrayViewColumn::to_array_column(column));
                //         LOG(INFO) << "convert done";
                //         new_column->check_or_die();
                //         // auto array_view_column = down_cast<const ArrayViewColumn*>(column.get());
                //         // ASSIGN_OR_RETURN(auto new_column, array_view_column->to_array_column());
                //         LOG(INFO) << "update column, idx: " << idx;
                //         tmp_chunk->update_column_by_index(new_column, idx);
                //     }
                // }
                tmp_chunk->check_or_die();
                // for (const auto& column: tmp_chunk->columns()) {
                //     LOG(INFO) << "column: " << column->get_name();
                //     DCHECK(!column->is_array_view()) << "unexpected array view";
                // }

                ASSIGN_OR_RETURN(auto tmp_col, context->evaluate(_children[0], tmp_chunk.get()));
                tmp_col->check_or_die();
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
    // @TODO handle const?

    // @TODO aligned offsets maybe null

    // attach offsets
    auto array_col = std::make_shared<ArrayColumn>(
            column, ColumnHelper::as_column<UInt32Column>(aligned_offsets->clone_shared()));
    array_col->check_or_die();
    if (result_null_column != nullptr) {
        return NullableColumn::create(std::move(array_col), result_null_column);
    }
    return array_col;
}

std::string ArrayMapExpr::debug_string() const {
    std::stringstream out;
    auto expr_debug_string = Expr::debug_string();
    out << "array_map (";
    for (int i = 0;i < _children.size();i++) {
        out << (i == 0 ? "": ", ") << _children[i]->debug_string();
    }
    out << ")";
    return out.str();
}

} // namespace starrocks
