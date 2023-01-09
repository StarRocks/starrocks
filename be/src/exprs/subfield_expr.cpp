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

#include "exprs/subfield_expr.h"

#include <cstring>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"

namespace starrocks {

class SubfieldExpr final : public Expr {
public:
    explicit SubfieldExpr(const TExprNode& node) : Expr(node), _used_subfield_name(node.used_subfield_name) {}

    SubfieldExpr(const SubfieldExpr&) = default;
    SubfieldExpr(SubfieldExpr&&) = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        DCHECK_EQ(1, _children.size());

        ASSIGN_OR_RETURN(ColumnPtr col, _children.at(0)->evaluate_checked(context, chunk));

        // handle nullable column
        std::vector<uint8_t> null_flags;
        size_t num_rows = col->size();
        null_flags.resize(num_rows, false);
        if (col->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(col.get());
            const uint8_t* nulls = nullable->null_column()->raw_data();
            // for (size_t i = 0; i < num_rows; i++) {
            //     null_flags[i] = nulls[i];
            // }
            std::memcpy(&null_flags[0], &nulls[0], num_rows * sizeof(uint8_t));
        }

        Column* tmp_col = ColumnHelper::get_data_column(col.get());
        DCHECK(tmp_col->is_struct());
        auto* struct_column = down_cast<StructColumn*>(tmp_col);

        ColumnPtr subfield_column = struct_column->field_column(_used_subfield_name);
        if (subfield_column->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(subfield_column.get());
            const uint8_t* nulls = nullable->null_column()->raw_data();
            for (size_t i = 0; i < num_rows; i++) {
                null_flags[i] |= nulls[i];
            }
            subfield_column = nullable->data_column();
        }

        NullColumnPtr result_null = NullColumn::create();
        result_null->get_data().swap(null_flags);
        DCHECK_EQ(subfield_column->size(), result_null->size());

        // If you want to edit subfield column, you need clone it first.
        return NullableColumn::create(subfield_column, result_null);
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new SubfieldExpr(*this)); }

private:
    std::string _used_subfield_name;
};

Expr* SubfieldExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::SUBFIELD_EXPR, node.node_type);
    DCHECK(node.__isset.used_subfield_name);
    return new SubfieldExpr(node);
}

} // namespace starrocks