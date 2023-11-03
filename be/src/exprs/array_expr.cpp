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

#include "exprs/array_expr.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"

namespace starrocks {

class ArrayExpr final : public Expr {
public:
    explicit ArrayExpr(const TExprNode& node) : Expr(node) {}

    ArrayExpr(const ArrayExpr&) = default;
    ArrayExpr(ArrayExpr&&) = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        const TypeDescriptor& element_type = _type.children[0];
        const size_t num_elements = _children.size();

        size_t num_rows = 1;
        // use chunk num_rows
        if (chunk) {
            num_rows = chunk->num_rows();
        }

        std::vector<ColumnPtr> element_columns(num_elements);
        std::vector<Column*> element_raw_ptrs(num_elements);
        for (size_t i = 0; i < num_elements; i++) {
            ASSIGN_OR_RETURN(auto col, _children[i]->evaluate_checked(context, chunk));
            num_rows = std::max(num_rows, col->size());
            element_columns[i] = std::move(col);
        }

        for (size_t i = 0; i < num_elements; i++) {
            element_columns[i] = ColumnHelper::unfold_const_column(element_type, num_rows, element_columns[i]);
            element_raw_ptrs[i] = element_columns[i].get();
        }

        auto array_elements = ColumnHelper::create_column(element_type, true);
        auto array_offsets = UInt32Column::create();

        // fill array column.
        uint32_t curr_offset = 0;
        array_offsets->append(curr_offset);
        for (size_t i = 0; i < num_rows; i++) {
            for (const Column* element : element_raw_ptrs) {
                array_elements->append(*element, i, 1);
            }
            curr_offset += num_elements;
            array_offsets->append(curr_offset);
        }

        return ArrayColumn::create(std::move(array_elements), std::move(array_offsets));
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ArrayExpr(*this)); }
};

Expr* ArrayExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::ARRAY_EXPR, node.node_type);
    DCHECK_GT(node.type.types.size(), 1);
    DCHECK_EQ(TTypeNodeType::ARRAY, node.type.types[0].type);
    return new ArrayExpr(node);
}

} // namespace starrocks
