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

#include <string>
#include <vector>

#include "common/statusor.h"
#include "exprs/jit/ir_helper.h"

namespace starrocks {

class Expr;
class ExprContext;
class RuntimeState;

class JITCodegenNode {
public:
    virtual ~JITCodegenNode() = default;

    virtual StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, JITContext* jit_ctx) = 0;
    virtual std::string jit_func_name_impl(RuntimeState* state) const = 0;
};

class ExprJITCodegen {
public:
    static StatusOr<LLVMDatum> generate_ir(ExprContext* context, Expr* expr, JITContext* jit_ctx);
    static std::string func_name(const Expr* expr, RuntimeState* state);
    static void collect_uncompilable_exprs(Expr* expr, std::vector<Expr*>& exprs, RuntimeState* state);
};

} // namespace starrocks
