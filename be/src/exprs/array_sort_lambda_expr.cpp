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

#include "exprs/array_sort_lambda_expr.h"

#include <fmt/format.h>

#include <algorithm>
#include <memory>
#include <sstream>

#include "base/simd/simd.h"
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
#include "storage/chunk_helper.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {
ArraySortLambdaExpr::ArraySortLambdaExpr(const TExprNode& node) : Expr(node, false) {}

ArraySortLambdaExpr::ArraySortLambdaExpr(TypeDescriptor type) : Expr(std::move(type), false) {}

Status ArraySortLambdaExpr::prepare(RuntimeState* state, ExprContext* context) {
    DCHECK(get_num_children() == 2);
    RETURN_IF_ERROR(_children[0]->prepare(state, context));

    auto lambda_fun = down_cast<LambdaFunction*>(_children[1]);
    LambdaFunction::ExtractContext extract_ctx;

    extract_ctx.next_slot_id = context->root()->max_used_slot_id() + 1;
    std::vector<SlotId> tmp_slots;
    lambda_fun->get_slot_ids(&tmp_slots);
    for (const auto id : tmp_slots) {
        _initial_required_slots.insert(id);
    }
    RETURN_IF_ERROR(lambda_fun->extract_outer_common_exprs(state, context, &extract_ctx));
    _outer_common_exprs.swap(extract_ctx.outer_common_exprs);

    for (auto [_, expr] : _outer_common_exprs) {
        RETURN_IF_ERROR(expr->prepare(state, context));
    }

    RETURN_IF_ERROR(lambda_fun->prepare(state, context));
    {
        // remove lambda arguments and common sub exprs from _initial_required_slots
        for (auto id : extract_ctx.all_lambda_arguments) {
            _initial_required_slots.erase(id);
        }
        for (auto id : extract_ctx.all_common_sub_expr_ids) {
            _initial_required_slots.erase(id);
        }
    }
    return Status::OK();
}

Status ArraySortLambdaExpr::open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));
    for (auto [_, expr] : _outer_common_exprs) {
        RETURN_IF_ERROR(expr->open(state, context, scope));
    }
    return Status::OK();
}

void ArraySortLambdaExpr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    for (auto [_, expr] : _outer_common_exprs) {
        expr->close(state, context, scope);
    }
    Expr::close(state, context, scope);
}

Status ArraySortLambdaExpr::do_for_each_child(const std::function<Status(Expr*)>& callback) {
    RETURN_IF_ERROR(Expr::do_for_each_child(callback));

    for (const auto& [slot_id, expr] : _outer_common_exprs) {
        RETURN_IF_ERROR(callback(expr));
    }
    return Status::OK();
}

// Helper class to manage comparisons during sorting

class SortComparator {
public:
    SortComparator(ExprContext* context, LambdaFunction* lambda_func, const ColumnPtr& elements_column,
                   std::shared_ptr<Chunk>& one_row_chunk)
            : _context(context),
              _lambda_func(lambda_func),
              _elements_column(elements_column),
              _one_row_chunk(one_row_chunk),
              _error_status(Status::OK()) {}

    void set_array_start(size_t start) { _array_start = start; }

    void set_argument_ids(const std::vector<SlotId>& argument_ids) { this->_argument_ids = argument_ids; }

    void set_constant_result(bool result) { _constant_result = result; }
    bool get_constant_result() const { return _constant_result; }

    bool compare(uint32_t i, uint32_t j) {
        if (!_error_status.ok()) {
            return false;
        }

        DCHECK(_argument_ids.size() == 2);
        // Get the argument columns from eval_chunk
        auto* arg1_col = _one_row_chunk->get_column_raw_ptr_by_slot_id(_argument_ids[0]);
        auto* arg2_col = _one_row_chunk->get_column_raw_ptr_by_slot_id(_argument_ids[1]);

        // Clear and rebuild the argument columns with just the two elements being compared
        arg1_col->reset_column();
        arg1_col->append(*_elements_column, _array_start + i, 1);
        arg2_col->reset_column();
        arg2_col->append(*_elements_column, _array_start + j, 1);

        for (const auto id : _lambda_func->get_common_sub_expr_ids()) {
            _one_row_chunk->remove_column_by_slot_id(id);
        }
        // Evaluate lambda function
        auto result_or = _context->evaluate(_lambda_func, _one_row_chunk.get());
        if (!result_or.ok()) {
            _error_status = result_or.status();
            return false;
        }

        auto result_col = result_or.value();
        if (result_col->is_null(0)) {
            _error_status = Status::InternalError("Comparator function returned NULL");
            return false;
        }

        auto* data_col = ColumnHelper::get_data_column(result_col.get());
        auto* boolean_col = down_cast<const BooleanColumn*>(data_col);
        DCHECK(boolean_col->size() == 1);
        return boolean_col->immutable_data()[0];
    }

    const Status& error_status() const { return _error_status; }

private:
    ExprContext* _context;
    LambdaFunction* _lambda_func;
    const ColumnPtr& _elements_column;
    mutable size_t _array_start{};
    mutable std::vector<SlotId> _argument_ids{};
    mutable bool _constant_result;
    std::shared_ptr<Chunk>& _one_row_chunk;
    Status _error_status{};
};

StatusOr<ColumnPtr> ArraySortLambdaExpr::evaluate_lambda_expr(ExprContext* context, Chunk* chunk,
                                                              const Column* data_column) {
    const auto& element_col = down_cast<const ArrayColumn*>(data_column)->elements_column();
    const auto& offsets_col = down_cast<const ArrayColumn*>(data_column)->offsets();

    auto lambda_func = dynamic_cast<LambdaFunction*>(_children[1]);
    std::vector<SlotId> capture_slot_ids;
    lambda_func->get_captured_slot_ids(&capture_slot_ids);

    // Create output arrays
    auto sorted_elements = element_col->clone_empty();
    auto sorted_offsets = UInt32Column::create();

    DCHECK(data_column->size() == 1 || data_column->size() == chunk->num_rows());

    const vector<SlotId>& argument_ids = lambda_func->get_lambda_arguments_ids();
    DCHECK(argument_ids.size() == 2);

    auto one_row_chunk = std::make_shared<Chunk>();
    auto captured_chunk = std::make_shared<Chunk>();

    for (auto arg_id : argument_ids) {
        auto arg_col = element_col->clone_empty();
        one_row_chunk->append_column(arg_col, arg_id);
    }

    // data_column may be a ArrayColumn wrapped in ConstColumn, if so, its size is 1;
    auto compute_once = data_column->size() == 1;

    SortComparator comparator(context, lambda_func, element_col, one_row_chunk);
    comparator.set_argument_ids(argument_ids);

    // if all columns that lambda depends on are constant or null, we only need to compute once
    const auto num_rows = compute_once ? 1 : chunk->num_rows();
    DCHECK(num_rows >= 1);
    if (data_column->size() == num_rows) {
        sorted_offsets->append(offsets_col, 0, offsets_col.size());
    } else {
        DCHECK(data_column->size() == 1);
        sorted_offsets->reserve(num_rows + 1);
        auto array_size = offsets_col.immutable_data()[1] - offsets_col.immutable_data()[0];
        for (auto i = 0; i < num_rows + 1; ++i) {
            sorted_offsets->append(array_size * i);
        }
    }

    // Process each row
    for (size_t row = 0; row < num_rows; ++row) {
        // if Array column comes from ConstColumn, always use the first offsets
        auto array_elem_idx = data_column->size() == 1 ? 0 : row;
        auto start_idx = offsets_col.immutable_data()[array_elem_idx];
        auto end_idx = offsets_col.immutable_data()[array_elem_idx + 1];

        uint32_t array_size = end_idx - start_idx;
        if (array_size == 0) {
            continue;
        }

        comparator.set_array_start(start_idx);

        for (const auto id : capture_slot_ids) {
            auto* column = captured_chunk->get_column_raw_ptr_by_slot_id(id);
            auto* col = one_row_chunk->get_column_raw_ptr_by_slot_id(id);
            col->reset_column();
            col->append(*column, row, 1);
        }

        // Create indices for sorting
        std::vector<uint32_t> indices(array_size);
        std::iota(indices.begin(), indices.end(), 0);

        // Sort using comparator lambda
        pdqsort(indices.begin(), indices.end(),
                [&comparator](uint32_t i, uint32_t j) { return comparator.compare(i, j); });
        // Check for any errors during comparison
        RETURN_IF_ERROR(comparator.error_status());

        // Add sorted elements to output
        for (uint32_t idx : indices) {
            sorted_elements->append(*element_col, start_idx + idx, 1);
        }
    }

    // Create output array column
    auto result_array = ArrayColumn::create(std::move(sorted_elements), std::move(sorted_offsets));
    result_array->check_or_die();
    if (compute_once) {
        auto const_result_array = ConstColumn::create(std::move(result_array), chunk->num_rows());
        return const_result_array;
    }
    return result_array;
}

struct ElemHolder {
    size_t index;
    const Column* column;
    uint32_t hash;
};

struct ElemHash {
    uint32_t operator()(const ElemHolder& elem) const { return elem.hash; }
};

struct ElemEqual {
    bool operator()(const ElemHolder& a, const ElemHolder& b) const {
        return a.column->compare_at(a.index, b.index, *(b.column), true) == 0;
    }
};

// Extract up to max_count unique elements from the array column
// Returns a vector of indices pointing to unique elements in the elements column
static std::vector<size_t> extract_unique_elements(const ArrayColumn* array_col, size_t max_count) {
    std::vector<size_t> unique_indices;
    const auto& element_col = array_col->elements_column();
    if (array_col->size() == 0) {
        return unique_indices;
    }
    phmap::flat_hash_set<ElemHolder, ElemHash, ElemEqual> unique_map;
    auto batch_size = max_count * 2;
    uint32_t hash_seeds[batch_size];
    std::fill_n(hash_seeds, batch_size, HashUtil::FNV_SEED);
    for (size_t i = 0; i < element_col->size() && unique_indices.size() < max_count; i += batch_size) {
        auto real_batch_size = std::min<size_t>(batch_size, element_col->size() - i);
        element_col->fnv_hash(hash_seeds, i, real_batch_size);
        for (size_t j = 0; j < real_batch_size && unique_indices.size() < max_count; ++j) {
            size_t idx = i + j;
            ElemHolder holder{idx, element_col.get(), hash_seeds[j]};
            auto result = unique_map.insert(holder);
            if (result.second) {
                unique_indices.push_back(idx);
            }
        }
    }
    return unique_indices;
}

Status ArraySortLambdaExpr::check_lambda_only_depends_on_args(const LambdaFunction* lambda_func) {
    std::vector<SlotId> capture_slot_ids;

    lambda_func->get_captured_slot_ids(&capture_slot_ids);
    if (!_initial_required_slots.empty() || !_outer_common_exprs.empty() || !capture_slot_ids.empty() ||
        lambda_func->is_lambda_expr_independent()) {
        return Status::InvalidArgument("Comparator lambda should only depends on its arguments");
    }
    return Status::OK();
}

// Validate strict weak ordering properties of the lambda comparator
Status ArraySortLambdaExpr::validate_strict_weak_ordering(ExprContext* context, Chunk* chunk,
                                                          const ArrayColumn* array_col) {
    // Check cache first to avoid duplicate validation
    if (_comparator_validated) {
        return _comparator_validation_status;
    }

    const auto& element_col = array_col->elements_column();
    auto lambda_func = dynamic_cast<LambdaFunction*>(_children[1]);

    // Extract up to 10 unique elements
    std::vector<size_t> unique_indices = extract_unique_elements(array_col, 10);

    if (unique_indices.empty()) {
        // No elements to validate, skip
        return Status::OK();
    }

    // Create a temporary array column with just the unique elements for validation
    auto validation_elements = element_col->clone_empty();
    for (size_t idx : unique_indices) {
        validation_elements->append(*element_col, idx, 1);
    }

    const vector<SlotId>& argument_ids = lambda_func->get_lambda_arguments_ids();
    DCHECK(argument_ids.size() == 2);

    auto one_row_chunk = std::make_shared<Chunk>();

    for (auto arg_id : argument_ids) {
        auto arg_col = element_col->clone_empty();
        one_row_chunk->append_column(arg_col, arg_id);
    }

    // Use SortComparator for validation
    SortComparator comparator(context, lambda_func, element_col, one_row_chunk);
    comparator.set_argument_ids(argument_ids);
    comparator.set_array_start(0); // Elements start at index 0 in validation array

    // Helper function to compare two elements by their indices in unique_indices
    auto compare = [&](size_t i, size_t j) -> StatusOr<bool> {
        if (!comparator.error_status().ok()) {
            return comparator.error_status();
        }
        bool result = comparator.compare(static_cast<uint32_t>(i), static_cast<uint32_t>(j));
        if (!comparator.error_status().ok()) {
            return comparator.error_status();
        }
        return result;
    };

    // Check 1: Irreflexivity - cmp(a, a) must be false
    for (size_t i = 0; i < unique_indices.size(); ++i) {
        ASSIGN_OR_RETURN(bool result, compare(i, i));
        if (result) {
            Status error = Status::InvalidArgument(
                    "Comparator violates irreflexivity: cmp(a, a) returned true for an element");
            _comparator_validated = true;
            _comparator_validation_status = error;
            return error;
        }
    }

    // Check 2: Asymmetry - for each pair (a, b), at most one of cmp(a, b) and cmp(b, a) is true
    for (size_t i = 0; i < unique_indices.size(); ++i) {
        for (size_t j = i + 1; j < unique_indices.size(); ++j) {
            ASSIGN_OR_RETURN(bool cmp_ij, compare(i, j));
            ASSIGN_OR_RETURN(bool cmp_ji, compare(j, i));

            if (cmp_ij && cmp_ji) {
                Status error = Status::InvalidArgument(
                        "Comparator violates asymmetry: both cmp(a, b) and cmp(b, a) returned true");
                _comparator_validated = true;
                _comparator_validation_status = error;
                return error;
            }
        }
    }

    // Check 3: Transitivity - check all permutations of (a, b, c)
    for (size_t i = 0; i < unique_indices.size(); ++i) {
        for (size_t j = 0; j < unique_indices.size(); ++j) {
            if (i == j) continue;
            for (size_t k = 0; k < unique_indices.size(); ++k) {
                if (i == k || j == k) continue;

                // Get all comparison results for the triple
                ASSIGN_OR_RETURN(bool cmp_ij, compare(i, j));
                ASSIGN_OR_RETURN(bool cmp_ji, compare(j, i));
                ASSIGN_OR_RETURN(bool cmp_jk, compare(j, k));
                ASSIGN_OR_RETURN(bool cmp_kj, compare(k, j));
                ASSIGN_OR_RETURN(bool cmp_ik, compare(i, k));
                ASSIGN_OR_RETURN(bool cmp_ki, compare(k, i));

                // Check transitivity for all permutations:
                // If cmp(i,j)=true and cmp(j,k)=true, then cmp(i,k) must be true
                if ((cmp_ij && cmp_jk && !cmp_ik) || (cmp_ji && cmp_ik && !cmp_jk) || (cmp_jk && cmp_ki && !cmp_ji) ||
                    (cmp_kj && cmp_ji && !cmp_ki) || (cmp_ik && cmp_kj && !cmp_ij) || (cmp_ki && cmp_ij && !cmp_kj)) {
                    Status error = Status::InvalidArgument(
                            "Comparator violates transitivity: cmp(a, b)=true and cmp(b, c)=true but "
                            "cmp(a, c)=false");
                    _comparator_validated = true;
                    _comparator_validation_status = error;
                    return error;
                }

                // Check equivalence transitivity: if all comparisons between i, j, k are false,
                // then all comparisons must be false (they form an equivalence class)
                if ((!cmp_ij && !cmp_ji && !cmp_jk && !cmp_kj && (cmp_ik || cmp_ki)) ||
                    (!cmp_ij && !cmp_ji && !cmp_ik && !cmp_ki && (cmp_jk || cmp_kj)) ||
                    (!cmp_jk && !cmp_kj && !cmp_ik && !cmp_ki && (cmp_ij || cmp_ji))) {
                    Status error = Status::InvalidArgument(
                            "Comparator violates incomparability transitivity: elements a, b, c are equivalent "
                            "(cmp(a,b)=false, cmp(b,a)=false, cmp(b,c)=false, cmp(c,b)=false) but "
                            "cmp(a,c) or cmp(c,a) returned true");
                    _comparator_validated = true;
                    _comparator_validation_status = error;
                    return error;
                }
            }
        }
    }

    // Cache the validation result
    _comparator_validated = true;
    _comparator_validation_status = Status::OK();
    return Status::OK();
}

// The input array column maybe nullable, so first remove the wrap of nullable property.
// The result of lambda expressions do not change the offsets of the current array and the null map.
// NOTE the return column must be of the return type.
StatusOr<ColumnPtr> ArraySortLambdaExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    // Evaluate array argument
    ASSIGN_OR_RETURN(auto array_col, context->evaluate(_children[0], chunk));

    // Handle null array
    if (array_col->only_null()) {
        return ColumnHelper::align_return_type(std::move(array_col), type(), chunk->num_rows(), true);
    }

    NullColumnPtr null_column = nullptr;
    auto* data_column = ColumnHelper::get_data_column(array_col.get());
    DCHECK(data_column->is_array() && !data_column->is_nullable() && !data_column->is_constant());
    if (array_col->is_nullable()) {
        null_column = down_cast<const NullableColumn*>(array_col.get())->null_column();
        const auto& offsets = down_cast<const ArrayColumn*>(data_column)->offsets();
        data_column->as_mutable_raw_ptr()->empty_null_in_complex_column(null_column->immutable_data(),
                                                                        offsets.immutable_data());
    }
    const auto* lambda_fun = down_cast<LambdaFunction*>(_children[1]);
    RETURN_IF_ERROR(check_lambda_only_depends_on_args(lambda_fun));
    // Validate strict weak ordering if lambda depends on arguments (i.e., it's a comparator)
    RETURN_IF_ERROR(validate_strict_weak_ordering(context, chunk, down_cast<const ArrayColumn*>(data_column)));

    return evaluate_lambda_expr(context, chunk, data_column);
}

std::string ArraySortLambdaExpr::debug_string() const {
    std::stringstream out;
    out << "array_sort_lambda (";
    for (int i = 0; i < _children.size(); i++) {
        out << (i == 0 ? "" : ", ") << _children[i]->debug_string();
    }
    out << ")";
    return out.str();
}

int ArraySortLambdaExpr::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    int num = Expr::get_slot_ids(slot_ids);
    for (const auto& [slot_id, expr] : _outer_common_exprs) {
        slot_ids->push_back(slot_id);
        num++;
        num += (expr->get_slot_ids(slot_ids));
    }
    return num;
}
} // namespace starrocks
