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

#include "exprs/jit/jit_expr.h"

#include <chrono>
#include <vector>

#include "column/chunk.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "exprs/function_context.h"
#include "exprs/jit/jit_functions.h"
#include "exprs/jit/jit_wrapper.h"
#include "llvm/IR/IRBuilder.h"

namespace starrocks {

JITExpr* JITExpr::create(ObjectPool* pool, Expr* expr) {
    TExprNode node;
    node.node_type = TExprNodeType::JIT_EXPR;
    node.opcode = TExprOpcode::JIT;
    node.is_nullable = expr->is_nullable();
    node.type = expr->type().to_thrift();
    node.output_scale = expr->output_scale();
    node.is_monotonic = expr->is_monotonic();
    return pool->add(new JITExpr(pool, node, expr));
}

JITExpr::JITExpr(ObjectPool* pool, const TExprNode& node, Expr* expr) : Expr(node), _pool(pool), _expr(expr) {
    _expr->get_uncompilable_exprs(_children);
}

Status JITExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> args_types;

    for (Expr* child : _children) {
        args_types.push_back(AnyValUtil::column_type_to_type_desc(child->type()));
    }
    _fn_context_index = context->register_func(state, return_type, args_types);

    return Status::OK();
}

Status JITExpr::open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

    // TODO(Yueyang): remove this time cost.
    auto start = std::chrono::high_resolution_clock::now();

    // Compile the expression into native code and retrieve the function pointer.
    auto* jit_wapper = JITWapper::get_instance();
    if (!jit_wapper->initialized()) {
        return Status::JitCompileError("JIT is not supported");
    }

    auto function = jit_wapper->compile_scalar_function(context, _expr);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    if (!function.ok()) {
        LOG(INFO) << "JIT: JIT compile failed, time cost: " << elapsed.count() << " s"
                  << " Reason: " << function.status();
    } else {
        LOG(INFO) << "JIT: JIT compile success, time cost: " << elapsed.count() << " s";
    }

    auto function_state = _pool->add(new JITFunctionState{function.value_or(nullptr)});

    // TODO(Yueyang): check THREAD_LOCAL or FRAGMENT_LOCAL.
    fn_ctx->set_function_state(FunctionContext::FunctionStateScope::THREAD_LOCAL, function_state);

    return Status::OK();
}

StatusOr<ColumnPtr> JITExpr::evaluate_checked(starrocks::ExprContext* context, Chunk* ptr) {
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    auto state = reinterpret_cast<JITFunctionState*>(fn_ctx->get_function_state(FunctionContext::THREAD_LOCAL));
    // If the expr fails to compile, evaluate using the original expr.
    if (UNLIKELY(state->_function == nullptr)) {
        LOG(ERROR) << "JIT: JIT compile failed, fallback to original expr";
        // TODO(Yueyang): fallback to original expr.
        return _expr->evaluate_checked(context, ptr);
    }

    Columns args;
    args.reserve(_children.size() + 1);
    for (Expr* child : _children) {
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, child, ptr);
        args.emplace_back(column);
    }

    // #ifndef NDEBUG
    if (ptr != nullptr) {
        size_t size = ptr->num_rows();
        // Ensure all columns have the same size
        for (const ColumnPtr& c : args) {
            CHECK_EQ(size, c->size());
        }
    }
    // #endif

    auto result_column = ColumnHelper::create_column(type(), is_nullable(), is_constant(), ptr->num_rows(), false);
    args.emplace_back(result_column);

    RETURN_IF_ERROR(JITFunction::llvm_function(fn_ctx, args));

    if (result_column->is_constant() && ptr != nullptr) {
        result_column->resize(ptr->num_rows());
    }
    RETURN_IF_ERROR(result_column->unfold_const_children(_type));
    return result_column;
}

void JITExpr::close() {
    auto* jit_wapper = JITWapper::get_instance();
    if (jit_wapper->initialized()) {
        jit_wapper->remove_function(_expr->debug_string());
    }

    Expr::close();
}

} // namespace starrocks
