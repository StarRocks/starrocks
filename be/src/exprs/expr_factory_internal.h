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

#include "common/status.h"

namespace starrocks {

class Expr;
class ObjectPool;
class RuntimeState;
class TExprNode;

class ExprFactoryInternal {
public:
    using NonCoreExprCreator = Status (*)(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                          RuntimeState* state);
    using JITRewriteHook = Status (*)(Expr** root_expr, ObjectPool* pool, RuntimeState* state);

    static void register_non_core_expr_creator(NonCoreExprCreator creator);
    static Status try_create_non_core_expr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
                                           RuntimeState* state);

    static void register_jit_rewrite_hook(JITRewriteHook hook);
    static Status try_rewrite_root_with_jit(Expr** root_expr, ObjectPool* pool, RuntimeState* state);
};

} // namespace starrocks
