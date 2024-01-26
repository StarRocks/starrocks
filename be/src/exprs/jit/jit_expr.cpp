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
    _is_prepared = true;

    auto start = MonotonicNanos();

    // Compile the expression into native code and retrieve the function pointer.
    auto* jit_engine = JITEngine::get_instance();
    if (!jit_engine->initialized()) {
        return Status::JitCompileError("JIT is not supported");
    }

    bool cached = false;
    auto function = jit_engine->compile_scalar_function(context, _expr, &cached);

    auto elapsed = MonotonicNanos() - start;
    if (!function.ok()) {
        LOG(INFO) << "JIT: JIT compile failed, time cost: " << elapsed / 1000000.0 << " ms"
                  << " Reason: " << function.status();
    } else if (!cached){
        LOG(INFO) << "JIT: JIT compile success, time cost: " << elapsed / 1000000.0 << " ms";
    }

    _jit_function = function.value_or(nullptr);
    if (_jit_function) {
        _jit_expr_name = _expr->debug_string();
        if (_jit_expr_name.empty()) {
            return Status::RuntimeError("expr debug_string() is empty");
        }
    }
    return Status::OK();
}

StatusOr<ColumnPtr> JITExpr::evaluate_checked(starrocks::ExprContext* context, Chunk* ptr) {
    // If the expr fails to compile, evaluate using the original expr.
    if (UNLIKELY(_jit_function == nullptr || ptr->num_rows() == 0)) {
        LOG(ERROR) << "JIT: JIT compile failed, fallback to original expr";
        // TODO(Yueyang): fallback to original expr perfectly.
        return _expr->evaluate_checked(context, ptr);
    }

    std::vector<JITColumn> jit_columns;
    jit_columns.reserve(_children.size() + 1);
    Columns backup_args;
    backup_args.reserve(_children.size() + 1);
    auto unfold_ptr = [&](ColumnPtr column) {
        DCHECK(!column->is_constant());
        auto [un_col, un_col_null] = ColumnHelper::unpack_nullable_column(column);
        auto data_col_ptr = reinterpret_cast<const int8_t*>(un_col->raw_data());
        const int8_t* null_flags_ptr = nullptr;
        if (un_col_null != nullptr) {
            null_flags_ptr = reinterpret_cast<const int8_t*>(un_col_null->raw_data());
        }
        jit_columns.emplace_back(JITColumn{data_col_ptr, null_flags_ptr});
    };
    for (Expr* child : _children) {
        // unfolding const columns.
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, child, ptr);
        DCHECK(column->size() == ptr->num_rows());
        if (UNLIKELY((column->is_constant() ^ child->is_constant()) ||
                     (column->is_nullable() ^ child->is_nullable()))) {
            LOG(INFO) << "[JIT INPUT] expr const = " << child->is_constant() << " null= " << child->is_nullable()
                      << " but col const = " << column->is_constant() << " null = " << column->is_nullable()
                      << " expr= " << child->debug_string() << " col= " << column->get_name();
        }
        if (column->is_constant()) {
            column = ColumnHelper::unfold_const_column(child->type(), column->size(), column);
        }

        if (child->is_nullable() && !column->is_nullable()) {
            column = NullableColumn::create(column, NullColumn::create(column->size(), 0));
        } else if (!child->is_nullable() && column->is_nullable()) {
            if (column->has_null()) {
                return Status::RuntimeError("a non-nullable column has null values");
            }
        }

        unfold_ptr(column);
        backup_args.emplace_back(column);
    }
    auto result_column =
            ColumnHelper::create_column(type(), !is_constant() && is_nullable(), false, ptr->num_rows(), false);
    unfold_ptr(result_column);
    _jit_function(ptr->num_rows(), jit_columns.data());
    return result_column;
}

// only unregister once
JITExpr::~JITExpr() {
    if (_is_prepared && _jit_function != nullptr) {
        auto* jit_engine = JITEngine::get_instance();
        if (jit_engine->initialized()) {
            auto status = jit_engine->remove_function(_jit_expr_name);
            if (!status.ok()) {
                LOG(WARNING) << "JIT: remove function failed, reason: " << status;
            }
        }
    }
}

} // namespace starrocks
