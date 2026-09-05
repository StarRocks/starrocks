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

#include "exprs/jit/expr_jit_rewriter.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/jit/expr_jit_codegen.h"
#include "exprs/jit/expr_jit_types.h"
#include "exprs/jit/jit_engine.h"
#include "exprs/jit/jit_expr.h"
#include "runtime/runtime_state.h"

namespace starrocks {

bool ExprJITRewriter::should_compile(const Expr* expr, RuntimeState* state) {
    if (!expr->is_compilable(state) || expr->children().empty() || expr->is_constant()) {
        return false;
    }

    if (state->is_adaptive_jit()) {
        auto score = expr->compute_jit_score(state);
        auto valid = (score.score > score.num * kExprJitScoreRatio && score.num > 2);
        VLOG_QUERY << "JIT score expr: score = " << score.score << " / " << score.num << " = "
                   << score.score * 1.0 / score.num << " valid = " << valid << "  "
                   << ExprJITCodegen::func_name(expr, state);
        if (!valid) {
            return false;
        }
    }
    return true;
}

Status ExprJITRewriter::replace_compilable_exprs(Expr** expr, ObjectPool* pool, RuntimeState* state, bool& replaced) {
    if ((*expr)->node_type() == TExprNodeType::DICT_EXPR || (*expr)->node_type() == TExprNodeType::DICT_QUERY_EXPR ||
        (*expr)->node_type() == TExprNodeType::DICTIONARY_GET_EXPR ||
        (*expr)->node_type() == TExprNodeType::PLACEHOLDER_EXPR || (*expr)->node_type() == TExprNodeType::MATCH_EXPR) {
        return Status::OK();
    }
    DCHECK(JITEngine::get_instance()->support_jit());
    if (should_compile(*expr, state)) {
        auto* jit_expr = JITExpr::create(pool, *expr);
        jit_expr->set_uncompilable_children(state);
        *expr = jit_expr;
        replaced = true;
    }

    for (auto& child : (*expr)->mutable_children()) {
        RETURN_IF_ERROR(replace_compilable_exprs(&child, pool, state, replaced));
    }
    return Status::OK();
}

Status ExprJITRewriter::prepare_rewritten_jit_exprs(Expr* expr, RuntimeState* state, ExprContext* context) {
    if (expr == nullptr) {
        return Status::OK();
    }
    if (expr->node_type() == TExprNodeType::JIT_EXPR) {
        RETURN_IF_ERROR(static_cast<JITExpr*>(expr)->prepare_impl(state, context));
    }
    for (auto child : expr->children()) {
        RETURN_IF_ERROR(prepare_rewritten_jit_exprs(child, state, context));
    }
    return Status::OK();
}

} // namespace starrocks
