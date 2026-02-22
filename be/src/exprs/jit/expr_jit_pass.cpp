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

#include "exprs/jit/expr_jit_pass.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/jit/expr_jit_rewriter.h"
#include "runtime/runtime_state_helper.h"

namespace starrocks {

Status ExprJITPass::rewrite_root(Expr** root, ObjectPool* pool, RuntimeState* state) {
    if (state == nullptr || root == nullptr || *root == nullptr || !RuntimeStateHelper::is_jit_enabled(state)) {
        return Status::OK();
    }

    bool replaced = false;
    auto st = ExprJITRewriter::replace_compilable_exprs(root, pool, state, replaced);
    if (!st.ok()) {
        LOG(WARNING) << "Can't replace compilable exprs.\n" << st.message() << "\n" << (*root)->debug_string();
        // Fall back to the non-JIT path.
    }
    return Status::OK();
}

Status ExprJITPass::rewrite_context(ExprContext* context, ObjectPool* pool) {
    if (context == nullptr) {
        return Status::OK();
    }

    auto* state = context->runtime_state();
    if (state == nullptr || !RuntimeStateHelper::is_jit_enabled(state)) {
        return Status::OK();
    }

    auto* root = context->mutable_root();
    if (root == nullptr || *root == nullptr) {
        return Status::OK();
    }
    bool replaced = false;
    auto st = ExprJITRewriter::replace_compilable_exprs(root, pool, state, replaced);
    if (!st.ok()) {
        LOG(WARNING) << "Can't replace compilable exprs.\n" << st.message() << "\n" << (*root)->debug_string();
        // Fall back to the non-JIT path.
        return Status::OK();
    }

    if (replaced) {
        auto prepare_st = ExprJITRewriter::prepare_rewritten_jit_exprs(*root, state, context);
        if (!prepare_st.ok()) {
            LOG(WARNING) << "prepare rewritten expr failed: " << prepare_st.message();
        }
    }

    return Status::OK();
}

} // namespace starrocks
