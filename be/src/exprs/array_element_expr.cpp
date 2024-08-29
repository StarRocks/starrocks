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

#include "exprs/array_element_expr.h"

#include <gutil/strings/substitute.h>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "util/raw_container.h"

namespace starrocks {

class ArrayElementExpr final : public Expr {
public:
    explicit ArrayElementExpr(const TExprNode& node, const bool check_is_out_of_bounds) : Expr(node) {
        _check_is_out_of_bounds = check_is_out_of_bounds;
    }

    ArrayElementExpr(const ArrayElementExpr&) = default;
    ArrayElementExpr(ArrayElementExpr&&) = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        DCHECK_EQ(2, _children.size());
        // After DLA's complex type prune, ArrayElement expr's type is different from children's type
        // DCHECK_EQ(_type, _children[0]->type().children[0]);
        ASSIGN_OR_RETURN(ColumnPtr arg0, _children[0]->evaluate_checked(context, chunk));
        ASSIGN_OR_RETURN(ColumnPtr arg1, _children[1]->evaluate_checked(context, chunk));
        size_t num_rows = std::max(arg0->size(), arg1->size());
        // No optimization for const column now.
        arg0 = ColumnHelper::unfold_const_column(_children[0]->type(), num_rows, arg0);
        arg1 = ColumnHelper::unfold_const_column(_children[1]->type(), num_rows, arg1);
        auto* array_column = down_cast<ArrayColumn*>(get_data_column(arg0.get()));
        auto* array_elements = array_column->elements_column().get();
        auto* array_elements_data = get_data_column(array_elements);
        DCHECK_EQ(num_rows, arg0->size());
        DCHECK_EQ(num_rows, arg1->size());
        DCHECK_EQ(num_rows + 1, array_column->offsets_column()->size());

        const int32_t* subscripts = down_cast<Int32Column*>(get_data_column(arg1.get()))->get_data().data();
        const uint32_t* offsets = array_column->offsets_column()->get_data().data();

        if (_check_is_out_of_bounds) {
            uint32_t prev = offsets[0];
            for (size_t i = 1; i <= num_rows; i++) {
                uint32_t curr = offsets[i];
                DCHECK_GE(curr, prev);
                auto subscript = (uint32_t)subscripts[i - 1];
                if (subscript == 0) {
                    return Status::InvalidArgument("Array subscript start at 1");
                }

                // if curr==prev, means this line is null
                // in Trino, null row's any subscript is still null
                if ((curr != prev) && (subscript > (curr - prev))) {
                    return Status::InvalidArgument(
                            strings::Substitute("Array subscript must be less than or equal to array length: $0 > $1",
                                                subscript, curr - prev));
                }
                prev = curr;
            }
        }

        std::vector<uint8_t> null_flags;
        raw::make_room(&null_flags, num_rows);

        // Construct null flags.
        uint32_t prev = offsets[0];
        for (size_t i = 1; i <= num_rows; i++) {
            uint32_t curr = offsets[i];
            DCHECK_GE(curr, prev);
            // cast negative subscript to large integer.
            auto subscript = (uint32_t)subscripts[i - 1];
            null_flags[i - 1] = (subscript > (curr - prev)) | (subscript == 0);
            prev = curr;
        }

        if (auto* nullable = dynamic_cast<NullableColumn*>(arg0.get()); nullable != nullptr) {
            const uint8_t* nulls = nullable->null_column()->raw_data();
            for (size_t i = 0; i < num_rows; i++) {
                null_flags[i] |= nulls[i];
            }
        }
        if (auto* nullable = dynamic_cast<NullableColumn*>(arg1.get()); nullable != nullptr) {
            const uint8_t* nulls = nullable->null_column()->raw_data();
            for (size_t i = 0; i < num_rows; i++) {
                null_flags[i] |= nulls[i];
            }
        }

        // construct selection list.
        std::vector<uint32_t> selection;
        starrocks::raw::make_room(&selection, num_rows);

        prev = offsets[0];
        uint32_t idx = 0;
        for (size_t i = 1; i <= num_rows; i++) {
            uint32_t curr = offsets[i];
            idx = null_flags[i - 1] ? idx : prev + (subscripts[i - 1] - 1);
            selection[i - 1] = idx;
            prev = curr;
        }
        DCHECK_EQ(num_rows, selection.size());

        if (array_elements->has_null()) {
            auto* nullable_elements = down_cast<NullableColumn*>(array_elements);
            const uint8_t* nulls = nullable_elements->null_column()->raw_data();
            for (size_t i = 0; i < num_rows; i++) {
                null_flags[i] |= nulls[selection[i]];
            }
        }

        // Construct the final result column;
        ColumnPtr result_data = array_elements_data->clone_empty();
        NullColumnPtr result_null = NullColumn::create();
        result_null->get_data().swap(null_flags);

        if (!array_elements_data->empty()) {
            result_data->append_selective(*array_elements_data, selection.data(), 0, num_rows);
        } else {
            result_data->append_default(num_rows);
        }
        DCHECK_EQ(result_null->size(), result_data->size());

        return NullableColumn::create(std::move(result_data), std::move(result_null));
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ArrayElementExpr(*this)); }

private:
    Column* get_data_column(Column* column) { return ColumnHelper::get_data_column(column); }
    bool _check_is_out_of_bounds = false;
};

Expr* ArrayElementExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::ARRAY_ELEMENT_EXPR, node.node_type);
    bool check_is_out_of_bounds = false;
    if (node.__isset.check_is_out_of_bounds) {
        check_is_out_of_bounds = node.check_is_out_of_bounds;
    }
    return new ArrayElementExpr(node, check_is_out_of_bounds);
}

} // namespace starrocks
