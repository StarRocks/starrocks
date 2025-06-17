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
#include "exprs/function_helper.h"

namespace starrocks {

class SubfieldExpr final : public Expr {
public:
    explicit SubfieldExpr(const TExprNode& node) : Expr(node), _used_subfield_names(node.used_subfield_names) {
        if (node.__isset.copy_flag) {
            _copy_flag = node.copy_flag;
        }
    }

    SubfieldExpr(const SubfieldExpr&) = default;
    SubfieldExpr(SubfieldExpr&&) = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        DCHECK_EQ(1, _children.size());

        ASSIGN_OR_RETURN(ColumnPtr col, _children.at(0)->evaluate_checked(context, chunk));

        // handle nullable column
        const size_t num_rows = col->size();
        if (col->only_null()) {
            return ColumnHelper::create_const_null_column(num_rows);
        }

        NullColumnPtr union_null_column = NullColumn::create(num_rows, false);

        for (size_t i = 0; i < _used_subfield_names.size(); i++) {
            const std::string& fieldname = _used_subfield_names[i];

            // merge null flags for each level
            if (col->is_nullable()) {
                auto* nullable = down_cast<NullableColumn*>(col.get());
                union_null_column =
                        FunctionHelper::union_null_column(std::move(union_null_column), nullable->null_column());
            }

            Column* tmp_col = ColumnHelper::get_data_column(col.get());
            DCHECK(tmp_col->is_struct());
            auto* struct_column = down_cast<StructColumn*>(tmp_col);
            col = struct_column->field_column(fieldname);
            if (col == nullptr) {
                return Status::InternalError("Struct subfield name: " + fieldname + " not found!");
            }
        }

        if (col->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(col.get());
            union_null_column =
                    FunctionHelper::union_null_column(std::move(union_null_column), nullable->null_column());
            col = nullable->data_column();
        }

        DCHECK_EQ(col->size(), union_null_column->size());

        // We need to clone a new subfield column
        if (_copy_flag) {
            return NullableColumn::create(Column::mutate(std::move(col)), std::move(union_null_column));
        } else {
            return NullableColumn::create(std::move(col), std::move(union_null_column));
        }
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new SubfieldExpr(*this)); }

    int get_subfields(std::vector<std::vector<std::string>>* subfields) const override {
        if (subfields != nullptr) {
            subfields->push_back(_used_subfield_names);
        }
        return 1;
    }

private:
    std::vector<std::string> _used_subfield_names;
    bool _copy_flag = true;
};

Expr* SubfieldExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::SUBFIELD_EXPR, node.node_type);
    DCHECK(node.__isset.used_subfield_names);
    return new SubfieldExpr(node);
}

} // namespace starrocks
