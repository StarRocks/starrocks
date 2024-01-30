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
#include "exprs/jit/jit_engine.h"
#include "exprs/jit/jit_functions.h"
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
    return pool->add(new JITExpr(node, expr));
}

JITExpr::JITExpr(const TExprNode& node, Expr* expr) : Expr(node), _expr(expr) {
    _expr->get_uncompilable_exprs(_children);
}

Status JITExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));

    if (_is_prepared) {
        return Status::OK();
    }
    _is_prepared = true;

    if (!is_constant()) {
        auto start = MonotonicNanos();

        // Compile the expression into native code and retrieve the function pointer.
        auto* jit_engine = JITEngine::get_instance();
        if (!jit_engine->initialized()) {
            return Status::JitCompileError("JIT is not supported");
        }

        auto function = jit_engine->compile_scalar_function(context, _expr);

        auto elapsed = MonotonicNanos() - start;
        if (!function.ok()) {
            LOG(INFO) << "JIT: JIT compile failed, time cost: " << elapsed / 1000000.0 << " ms"
                      << " Reason: " << function.status();
        } else {
            LOG(INFO) << "JIT: JIT compile success, time cost: " << elapsed / 1000000.0 << " ms";
        }

        _jit_function = function.value_or(nullptr);
    }
    if (_jit_function != nullptr) {
        _jit_expr_name = _expr->debug_string();
    } else {
        _children.clear();
        _children.push_back(_expr);
        RETURN_IF_ERROR(Expr::prepare(state, context)); // jitExpr becomes an empty node, fallback to original expr.
    }
    return Status::OK();
}

StatusOr<ColumnPtr> JITExpr::evaluate_checked(starrocks::ExprContext* context, Chunk* ptr) {
    // If the expr fails to compile, evaluate using the original expr.
    if (UNLIKELY(_jit_function == nullptr)) {
        return _expr->evaluate_checked(context, ptr);
    }

    Columns args;
    args.reserve(_children.size() + 1);
    size_t num_rows = 0;
    for (Expr* child : _children) {
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, child, ptr);
        if (column->only_null()) { // TODO(Yueyang): remove this when support ifnull expr.
            return ColumnHelper::align_return_type(column, type(), column->size(), true);
        }
        args.emplace_back(column);
        num_rows = std::max<size_t>(num_rows, column->size());
    }
    if (ptr == nullptr) {
        if (is_constant() && num_rows == 0) {
            num_rows = 1;
        }
    } else {
        num_rows = ptr->num_rows();
    }

#ifdef DEBUG
    if (ptr != nullptr) {
        size_t size = ptr->num_rows();
        // Ensure all columns have the same size
        for (const ColumnPtr& c : args) {
            CHECK_EQ(size, c->size());
        }
    }
#endif

    auto result_column = ColumnHelper::create_column(type(), is_nullable(), false, num_rows, false);
    args.emplace_back(result_column);

    RETURN_IF_ERROR(JITFunction::llvm_function(_jit_function, args));

    if (result_column->is_constant() && ptr != nullptr) {
        result_column->resize(ptr->num_rows());
    }

    return result_column;
}

} // namespace starrocks