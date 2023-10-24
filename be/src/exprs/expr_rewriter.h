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

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {
class ExprRewriter {
public:
    ExprRewriter(ObjectPool* pool, Expr* expr) : _pool(pool), _expr(expr) {}
    // rewrite
    template <class Rewriter>
    StatusOr<Expr*> rewrite(Rewriter&& rewriter) {
        return _rewrite(rewriter, _expr, _pool);
    }

private:
    template <class Rewriter>
    StatusOr<Expr*> _rewrite(Rewriter&& rewriter, Expr* expr, ObjectPool* pool) {
        auto& children = expr->_children;
        ASSIGN_OR_RETURN(expr, rewriter(expr, pool));
        DCHECK(expr->children().empty());
        for (auto child : children) {
            ASSIGN_OR_RETURN(auto new_child, _rewrite(rewriter, child, pool));
            expr->add_child(new_child);
        }
        return expr;
    }

    ObjectPool* _pool;
    Expr* _expr;
};
} // namespace starrocks