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

#pragma once

#include "column/column.h"
#include "exprs/expr.h"

namespace starrocks {

class CloneExpr final : public Expr {
public:
    CloneExpr(const TExprNode& node) : Expr(node){};

    ~CloneExpr() override = default;

    Expr* clone(ObjectPool* pool) const override { return pool->add(new CloneExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(ColumnPtr column, get_child(0)->evaluate_checked(context, ptr));
        return column->clone();
    }

    static Expr* from_child(Expr* child, ObjectPool* pool) {
        TExprNode clone_node;
        clone_node.node_type = TExprNodeType::CLONE_EXPR;
        clone_node.type = child->type().to_thrift();
        clone_node.__set_child_type(to_thrift(child->type().type));
        clone_node.__set_child_type_desc(child->type().to_thrift());
        Expr* clone_expr = pool->add(new CloneExpr(clone_node));
        clone_expr->add_child(child);
        return clone_expr;
    }
};
} // namespace starrocks
