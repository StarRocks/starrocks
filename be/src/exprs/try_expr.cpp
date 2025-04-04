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

#include "exprs/try_expr.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"

namespace starrocks {

class TryExpr final : public Expr {
public:
    explicit TryExpr(const TExprNode& node) : Expr(node) {}

    TryExpr(const TryExpr&) = default;
    TryExpr(TryExpr&&) = default;

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        auto&& status = _children[0]->evaluate_checked(context, chunk);
        if (LIKELY(status.ok())) {
            return status;
        }
        return ColumnHelper::create_const_null_column(chunk->num_rows());
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new TryExpr(*this)); }
};

Expr* TryExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::TRY_EXPR, node.node_type);
    DCHECK_EQ(node.type.types.size(), 1);
    return new TryExpr(node);
}

} // namespace starrocks
