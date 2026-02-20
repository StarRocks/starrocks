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

#include <vector>

#include "common/status.h"

namespace starrocks {

class Expr;
class ExprContext;
class ObjectPool;
class RuntimeState;
class TExpr;
class TExprNode;

class ExprFactory {
public:
    static Status create_expr_tree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr, RuntimeState* state,
                                   bool can_jit = false);

    static Status create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs, std::vector<Expr*>* root_exprs,
                                    RuntimeState* state, bool can_jit = false);

    static Status create_expr_from_thrift_nodes(ObjectPool* pool, const std::vector<TExprNode>& nodes, int* node_idx,
                                                Expr** root_expr, RuntimeState* state, bool can_jit = false);

    // Compatibility overload for call sites that still materialize ExprContext directly.
    static Status create_expr_tree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx, RuntimeState* state,
                                   bool can_jit = false);

    // Compatibility overload for call sites that still materialize ExprContext directly.
    static Status create_expr_trees(ObjectPool* pool, const std::vector<TExpr>& texprs, std::vector<ExprContext*>* ctxs,
                                    RuntimeState* state, bool can_jit = false);
};

} // namespace starrocks
