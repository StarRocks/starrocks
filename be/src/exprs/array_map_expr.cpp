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
#include <sstream>

#include "column/array_column.h"
#include "column/array_view_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/constexpr.h"
#include "common/statusor.h"
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
    for (int i = 1; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state, context));
    }

    auto lambda_expr = down_cast<LambdaFunction*>(_children[0]);
    LambdaFunction::ExtractContext extract_ctx;
    // assign slot ids to outer common exprs starting with max_used_slot_id + 1
    extract_ctx.next_slot_id = context->root()->max_used_slot_id() + 1;

    RETURN_IF_ERROR(lambda_expr->extract_outer_common_exprs(state, context, &extract_ctx));
    _outer_common_exprs.swap(extract_ctx.outer_common_exprs);
    for (auto [_, expr] : _outer_common_exprs) {
        RETURN_IF_ERROR(expr->prepare(state, context));
    }
    RETURN_IF_ERROR(lambda_expr->prepare(state, context));

    return Status::OK();
}
Status ArrayMapExpr::open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));
    for (auto [_, expr] : _outer_common_exprs) {
        RETURN_IF_ERROR(expr->open(state, context, scope));
    }
    return Status::OK();
}

void ArrayMapExpr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    for (auto [_, expr] : _outer_common_exprs) {
        expr->close(state, context, scope);
    }
    Expr::close(state, context, scope);
}

template <bool all_const_input, bool independent_lambda_expr>
StatusOr<ColumnPtr> ArrayMapExpr::evaluate_lambda_expr(ExprContext* context, Chunk* chunk,
                                                       const std::vector<ColumnPtr>& input_elements,
                                                       const NullColumnPtr& result_null_column) {
    // create a new chunk to evaluate the lambda expression
    auto cur_chunk = std::make_shared<Chunk>();
    auto tmp_chunk = std::make_shared<Chunk>();
    {
        // see more details: https://github.com/StarRocks/starrocks/pull/52692
        for (const auto& [slot_id, _] : chunk->get_slot_id_to_index_map()) {
            tmp_chunk->append_column(chunk->get_column_by_slot_id(slot_id), slot_id);
        }
    }

    // 1. evaluate outer common expressions
    for (const auto& [slot_id, expr] : _outer_common_exprs) {
        ASSIGN_OR_RETURN(auto col, context->evaluate(expr, tmp_chunk.get()));
        tmp_chunk->append_column(col, slot_id);
    }

    auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
    std::vector<SlotId> capture_slot_ids;
    lambda_func->get_captured_slot_ids(&capture_slot_ids);

    // 2. check captured columns' size
    for (auto slot_id : capture_slot_ids) {
        DCHECK(slot_id > 0);
        auto captured_column = chunk->is_slot_exist(slot_id) ? chunk->get_column_by_slot_id(slot_id)
                                                             : tmp_chunk->get_column_by_slot_id(slot_id);
        if (UNLIKELY(captured_column->size() < input_elements[0]->size())) {
            return Status::InternalError(fmt::format("The size of the captured column {} is less than array's size.",
                                                     captured_column->get_name()));
        }
    }

    UInt32Column::Ptr aligned_offsets = nullptr;
    size_t null_rows = result_null_column ? SIMD::count_nonzero(result_null_column->get_data()) : 0;

    std::vector<SlotId> arguments_ids;
    int argument_num = lambda_func->get_lambda_arguments_ids(&arguments_ids);

    // 3. prepare arguments of lambda expr, put all arguments into cur_chunk
    for (int i = 0; i < argument_num; ++i) {
        auto data_column = FunctionHelper::get_data_column_of_const(input_elements[i]);
        auto array_column = down_cast<const ArrayColumn*>(data_column.get());
        auto elements_column = array_column->elements_column();
        UInt32Column::Ptr offsets_column = array_column->offsets_column();

        if (input_elements[i]->is_constant()) {
            if (!all_const_input || !capture_slot_ids.empty()) {
                // if not all input columns are constant,
                // or all input columns are constant but lambda expr depends on other capture columns(e.g. array_map(x->x+k,[1,2,3])),
                // we should unpack the const column before evaluation
                size_t elements_num = array_column->get_element_size(0);
                elements_column = elements_column->clone();
                offsets_column = UInt32Column::create();
                // replicate N time and ignore null
                size_t repeat_times = input_elements[i]->size() - null_rows;
                size_t offset = elements_num;
                offsets_column->append(0);
                offsets_column->append(offset);
                for (size_t i = 1; i < repeat_times; i++) {
                    elements_column->append(*elements_column, 0, elements_num);
                    offset += elements_num;
                    offsets_column->append(offset);
                }
            }
        } else {
            if (result_null_column != nullptr) {
                data_column->empty_null_in_complex_column(result_null_column->get_data(),
                                                          array_column->offsets().get_data());
            }
            elements_column = down_cast<const ArrayColumn*>(data_column.get())->elements_column();
        }

        if (aligned_offsets == nullptr) {
            aligned_offsets = offsets_column;
        }

        // if lambda expr doesn't rely on argument, we don't need to put it into cur_chunk
        if constexpr (!independent_lambda_expr) {
            cur_chunk->append_column(elements_column, arguments_ids[i]);
        }
    }
    DCHECK(aligned_offsets != nullptr);

    // 4. prepare capture columns
    for (auto slot_id : capture_slot_ids) {
        auto captured_column = chunk->is_slot_exist(slot_id) ? chunk->get_column_by_slot_id(slot_id)
                                                             : tmp_chunk->get_column_by_slot_id(slot_id);
        if constexpr (independent_lambda_expr) {
            cur_chunk->append_column(captured_column, slot_id);
        } else {
            if (captured_column->is_array()) {
                auto view_column = ArrayViewColumn::from_array_column(captured_column);
                ASSIGN_OR_RETURN(auto replicated_view_column, view_column->replicate(aligned_offsets->get_data()));
                cur_chunk->append_column(replicated_view_column, slot_id);
            } else {
                ASSIGN_OR_RETURN(auto replicated_column, captured_column->replicate(aligned_offsets->get_data()));
                cur_chunk->append_column(replicated_column, slot_id);
            }
        }
    }

    // 5. evaluate lambda expr
    ColumnPtr column = nullptr;
    if constexpr (independent_lambda_expr) {
        // if lambda expr doesn't rely on arguments, we evaluate it first, and then align offsets
        ColumnPtr tmp_col;
        if (!cur_chunk->has_columns()) {
            ASSIGN_OR_RETURN(tmp_col, context->evaluate(_children[0], nullptr));
        } else {
            ASSIGN_OR_RETURN(tmp_col, context->evaluate(_children[0], cur_chunk.get()));
        }
        tmp_col->check_or_die();
        ASSIGN_OR_RETURN(column, tmp_col->replicate(aligned_offsets->get_data()));
        column = ColumnHelper::align_return_type(column, type().children[0], column->size(), true);

        RETURN_IF_ERROR(column->capacity_limit_reached());
    } else {
        // if all input arguments are constant and lambda expr doesn't rely on other capture columns,
        // we can evaluate it based on const column
        if (all_const_input && capture_slot_ids.empty()) {
            ASSIGN_OR_RETURN(auto tmp_col, context->evaluate(_children[0], cur_chunk.get()));
            tmp_col->check_or_die();
            // if result is a const column, we should unpack it first and make it to be the elements column of array column
            column = ColumnHelper::unpack_and_duplicate_const_column(tmp_col->size(), tmp_col);
            column = ColumnHelper::align_return_type(column, type().children[0], column->size(), true);
        } else {
            ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
            RETURN_IF_ERROR(accumulator.push(std::move(cur_chunk)));
            accumulator.finalize();
            while (auto tmp_chunk = accumulator.pull()) {
                tmp_chunk->check_or_die();
                for (auto& column : tmp_chunk->columns()) {
                    // because not all functions can handle ArrayViewColumn correctly, we need to convert it back to ArrayColumn first.
                    // in the future, this copy can be removed when we solve this problem.
                    if (column->is_array_view()) {
                        column = ArrayViewColumn::to_array_column(column);
                    }
                }
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
    }

    DCHECK(column != nullptr);
    column = ColumnHelper::cast_to_nullable_column(column);

    if (all_const_input && capture_slot_ids.empty()) {
        // if all input arguments are const and lambdaexpr doesn't depend on other capture columns,
        // we can return a const column
        auto data_column = FunctionHelper::get_data_column_of_const(column);

        aligned_offsets = UInt32Column::create();
        aligned_offsets->append(0);
        aligned_offsets->append(data_column->size());
        auto array_column =
                std::make_shared<ArrayColumn>(data_column, ColumnHelper::as_column<UInt32Column>(aligned_offsets));
        array_column->check_or_die();
        ColumnPtr result_column = array_column;
        if (result_null_column != nullptr) {
            result_column = NullableColumn::create(std::move(array_column), result_null_column);
            result_column->check_or_die();
        }
        result_column = ConstColumn::create(result_column, chunk->num_rows());
        result_column->check_or_die();
        return result_column;
    } else {
        auto array_column = std::make_shared<ArrayColumn>(
                column, ColumnHelper::as_column<UInt32Column>(aligned_offsets->clone_shared()));
        array_column->check_or_die();
        if (result_null_column != nullptr) {
            return NullableColumn::create(std::move(array_column), result_null_column);
        }
        return array_column;
    }
}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions do not change the offsets of the current array and the null map.
// NOTE the return column must be of the return type.
StatusOr<ColumnPtr> ArrayMapExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    std::vector<ColumnPtr> input_elements;
    bool is_single_nullable_child = false;

    NullColumnPtr result_null_column = nullptr;
    bool all_input_is_constant = true;

    for (int i = 1; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(auto child_col, context->evaluate(_children[i], chunk));
        // the column is a null literal.
        if (child_col->only_null()) {
            return ColumnHelper::align_return_type(child_col, type(), chunk->num_rows(), true);
        }

        bool is_const = child_col->is_constant();
        bool is_nullable = child_col->is_nullable();
        size_t num_rows = child_col->size();
        all_input_is_constant &= is_const;

        auto data_column = child_col;
        if (is_const) {
            auto const_column = down_cast<const ConstColumn*>(child_col.get());
            data_column = const_column->data_column();
        }

        if (is_nullable) {
            auto nullable_column = down_cast<const NullableColumn*>(data_column.get());
            DCHECK(nullable_column);
            data_column = nullable_column->data_column();

            auto null_column = nullable_column->null_column();
            if (is_const) {
                // if null_column is from const_column, should unpack
                null_column->assign(num_rows, 0);
            }

            if (result_null_column) {
                is_single_nullable_child = false;
                result_null_column = FunctionHelper::union_null_column(null_column, result_null_column);
            } else {
                is_single_nullable_child = true;
                result_null_column = null_column;
            }
        }
        DCHECK(data_column->is_array() && !data_column->is_nullable());

        ColumnPtr column = data_column;
        if (is_const) {
            // keep it as a const array column in input_elements
            column = ConstColumn::create(data_column, num_rows);
        }

        // check each array's lengths in input_elements
        if (!input_elements.empty()) {
            const auto& first_input = input_elements[0];

            bool is_array_lengths_valid =
                    result_null_column
                            ? ArrayColumn::is_all_array_lengths_equal<false>(first_input, column, result_null_column)
                            : ArrayColumn::is_all_array_lengths_equal<true>(first_input, column, result_null_column);
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
    size_t null_rows = result_null_column ? SIMD::count_nonzero(result_null_column->get_data()) : 0;

    if (null_rows == input_elements[0]->size()) {
        // if all input rows are null, just return a const nullable array column as result
        column = ColumnHelper::create_column(type().children[0],
                                             true); // array->elements must be of return array->elements' type
        column->append_default(1);
        auto aligned_offsets = UInt32Column::create(0);
        aligned_offsets->append(0);
        aligned_offsets->append(1);
        auto array_col = std::make_shared<ArrayColumn>(column, aligned_offsets);
        array_col->check_or_die();
        if (result_null_column) {
            result_null_column->resize(1);
            auto result = ConstColumn::create(NullableColumn::create(std::move(array_col), result_null_column),
                                              chunk->num_rows());
            result->check_or_die();
            return result;
        }
        auto result = ConstColumn::create(std::move(array_col), chunk->num_rows());
        result->check_or_die();
        return result;
    }

    size_t total_elements_num =
            down_cast<ArrayColumn*>(FunctionHelper::get_data_column_of_const(input_elements[0]).get())
                    ->get_total_elements_num(result_null_column);

    if (total_elements_num == 0) {
        // if all input rows are empty arrays, return a const empty array column as result
        column = ColumnHelper::create_column(type().children[0], true);
        auto aligned_offsets = UInt32Column::create(0);
        aligned_offsets->append_default(2);
        auto array_col = std::make_shared<ArrayColumn>(column, aligned_offsets);
        array_col->check_or_die();
        auto result = ConstColumn::create(std::move(array_col), chunk->num_rows() - null_rows);
        result->check_or_die();
        return result;
    }

    auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
    bool is_lambda_expr_independent = lambda_func->is_lambda_expr_independent();
    if (all_input_is_constant && is_lambda_expr_independent) {
        return evaluate_lambda_expr<true, true>(context, chunk, input_elements, result_null_column);
    } else if (all_input_is_constant && !is_lambda_expr_independent) {
        return evaluate_lambda_expr<true, false>(context, chunk, input_elements, result_null_column);
    } else if (!all_input_is_constant && is_lambda_expr_independent) {
        return evaluate_lambda_expr<false, true>(context, chunk, input_elements, result_null_column);
    } else {
        return evaluate_lambda_expr<false, false>(context, chunk, input_elements, result_null_column);
    }
}

std::string ArrayMapExpr::debug_string() const {
    std::stringstream out;
    auto expr_debug_string = Expr::debug_string();
    out << "array_map (";
    for (int i = 0; i < _children.size(); i++) {
        out << (i == 0 ? "" : ", ") << _children[i]->debug_string();
    }
    out << ")";
    return out.str();
}

int ArrayMapExpr::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    int num = Expr::get_slot_ids(slot_ids);
    for (const auto& [slot_id, expr] : _outer_common_exprs) {
        slot_ids->push_back(slot_id);
        num++;
        num += (expr->get_slot_ids(slot_ids));
    }
    return num;
}

} // namespace starrocks
