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

#include "common/object_pool.h"
#include "common/status.h"

namespace starrocks {

class Expr;
class ExprContext;
class RuntimeState;

class ExprJITRewriter {
public:
    static bool should_compile(const Expr* expr, RuntimeState* state);
    static Status replace_compilable_exprs(Expr** expr, ObjectPool* pool, RuntimeState* state, bool& replaced);
    static Status prepare_rewritten_jit_exprs(Expr* expr, RuntimeState* state, ExprContext* context);
};

} // namespace starrocks
