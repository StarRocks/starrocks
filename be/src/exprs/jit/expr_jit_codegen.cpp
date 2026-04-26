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

#include "exprs/jit/expr_jit_codegen.h"

#include <stdexcept>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {

StatusOr<LLVMDatum> generate_fallback_ir(ExprContext* context, Expr* expr, JITContext* jit_ctx) {
    if (expr->is_compilable(context->runtime_state())) {
#if BE_TEST
        throw std::runtime_error("[JIT] compilable expressions must not be here : " + expr->debug_string());
#else
        return Status::NotSupported("[JIT] compilable expressions must override generate_ir_impl()");
#endif
    }
    if (jit_ctx->input_index >= jit_ctx->columns.size() - 1) {
#if BE_TEST
        throw std::runtime_error("[JIT] vector overflow for expr :" + expr->debug_string());
#else
        return Status::RuntimeError("[JIT] vector overflow for uncompilable expr");
#endif
    }

    LLVMDatum datum(jit_ctx->builder);
    datum.value = jit_ctx->builder.CreateLoad(
            jit_ctx->columns[jit_ctx->input_index].value_type,
            jit_ctx->builder.CreateInBoundsGEP(jit_ctx->columns[jit_ctx->input_index].value_type,
                                               jit_ctx->columns[jit_ctx->input_index].values, jit_ctx->index_phi));
    if (expr->is_nullable()) {
        datum.null_flag = jit_ctx->builder.CreateLoad(
                jit_ctx->builder.getInt8Ty(),
                jit_ctx->builder.CreateInBoundsGEP(jit_ctx->builder.getInt8Ty(),
                                                   jit_ctx->columns[jit_ctx->input_index].null_flags,
                                                   jit_ctx->index_phi));
    }
    jit_ctx->input_index++;
    return datum;
}

std::string fallback_func_name(const Expr* expr, RuntimeState* state) {
    DCHECK(!expr->is_compilable(state));
    return std::string("col[") + (expr->is_constant() ? "c:" : "") + (expr->is_nullable() ? "n:" : "") +
           expr->type().debug_string() + "]";
}

} // namespace

StatusOr<LLVMDatum> ExprJITCodegen::generate_ir(ExprContext* context, Expr* expr, JITContext* jit_ctx) {
    if (expr->is_compilable(context->runtime_state())) {
        if (auto* codegen_node = dynamic_cast<JITCodegenNode*>(expr); codegen_node != nullptr) {
            return codegen_node->generate_ir_impl(context, jit_ctx);
        }
    }
    return generate_fallback_ir(context, expr, jit_ctx);
}

std::string ExprJITCodegen::func_name(const Expr* expr, RuntimeState* state) {
    if (expr->is_compilable(state)) {
        if (auto* codegen_node = dynamic_cast<const JITCodegenNode*>(expr); codegen_node != nullptr) {
            return codegen_node->jit_func_name_impl(state);
        }
    }
    return fallback_func_name(expr, state);
}

void ExprJITCodegen::collect_uncompilable_exprs(Expr* expr, std::vector<Expr*>& exprs, RuntimeState* state) {
    if (!expr->is_compilable(state)) {
        exprs.emplace_back(expr);
        return;
    }
    for (auto child : expr->children()) {
        collect_uncompilable_exprs(child, exprs, state);
    }
}

} // namespace starrocks
