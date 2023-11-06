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
#include "exprs/jit/jit_engine.h"
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

    if (_is_prepared) {
        return Status::OK();
    }

    if (_is_prepared) {
        return Status::OK();
    }
    if (prepared_times.fetch_add(1) > 0) {
        LOG(ERROR) << "prepared more times";
        return Status::RuntimeError("Prepared more times");
    }

    // TODO(Yueyang): remove this time cost.
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
    return Status::OK();
}

StatusOr<ColumnPtr> JITExpr::evaluate_checked(starrocks::ExprContext* context, Chunk* ptr) {
    // If the expr fails to compile, evaluate using the original expr.
    if (UNLIKELY(_jit_function == nullptr)) {
        LOG(ERROR) << "JIT: JIT compile failed, fallback to original expr";
        // TODO(Yueyang): fallback to original expr perfectly.
        return _expr->evaluate_checked(context, ptr);
    }

    Columns args;
    args.reserve(_children.size() + 1);
    for (Expr* child : _children) {
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, child, ptr);
        if (column->only_null()) { // TODO(Yueyang): remove this when support ifnull expr.
            return ColumnHelper::align_return_type(column, type(), column->size(), true);
        }
        args.emplace_back(column);
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

    auto result_column =
            ColumnHelper::create_column(type(), !is_constant() && is_nullable(), is_constant(), ptr->num_rows(), false);
    args.emplace_back(result_column);

    RETURN_IF_ERROR(JITFunction::llvm_function(_jit_function, args));

    if (result_column->is_constant() && ptr != nullptr) {
        result_column->resize(ptr->num_rows());
    }
    // RETURN_IF_ERROR(result_column->unfold_const_children(_type));
    return result_column;
}

// only unregister once
JITExpr::~JITExpr() {
    if (_is_prepared && _jit_function != nullptr) {
        auto* jit_engine = JITEngine::get_instance();
        if (jit_engine->initialized()) {
            auto status = jit_engine->remove_function(_expr->debug_string());
            if (!status.ok()) {
                LOG(WARNING) << "JIT: remove function failed, reason: " << status;
            }
        }
    }
}

} // namespace starrocks
