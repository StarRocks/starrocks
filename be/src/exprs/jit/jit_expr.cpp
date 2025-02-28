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

#include <llvm/IR/IRBuilder.h>

#include <chrono>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/expr.h"
#include "exprs/function_context.h"
#include "exprs/jit/jit_engine.h"
#include "runtime/runtime_state.h"

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

JITExpr::JITExpr(const TExprNode& node, Expr* expr) : Expr(node), _expr(expr) {}

void JITExpr::set_uncompilable_children(RuntimeState* state) {
    _children.clear();
    _expr->get_uncompilable_exprs(_children, state);
}

Status JITExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    RETURN_IF_ERROR(prepare_impl(state, context));
    if (_jit_function == nullptr) {
        _children.clear();
        _children.push_back(_expr);
        // jitExpr becomes an empty node, fallback to original expr, which are prepared again in case of jit
        // complex expressions later.
        RETURN_IF_ERROR(Expr::prepare(state, context));
    }
    return Status::OK();
}

Status JITExpr::prepare_impl(RuntimeState* state, ExprContext* context) {
    if (_is_prepared) {
        return Status::OK();
    }
    _is_prepared = true;

    if (!is_constant()) {
        auto start = MonotonicNanos();

        // Compile the expression into native code and retrieve the function pointer.
        auto* jit_engine = JITEngine::get_instance();
        if (!jit_engine->support_jit()) {
            return Status::JitCompileError("JIT is not supported");
        }
        auto expr_name = _expr->jit_func_name(state);
        _jit_obj_cache = std::make_unique<JitObjectCache>(expr_name, JITEngine::get_instance()->get_func_cache());

        auto st = jit_engine->compile_scalar_function(context, _jit_obj_cache.get(), _expr, _children);
        auto elapsed = MonotonicNanos() - start;
        if (state->fragment_ctx() != nullptr) {
            state->fragment_ctx()->update_jit_profile(elapsed);
        }
        if (!st.ok()) {
            LOG(INFO) << "JIT: JIT compile failed, time cost: " << elapsed / 1000000.0 << " ms"
                      << " Reason: " << st;
        } else {
            VLOG_QUERY << "JIT: JIT compile success, time cost: " << elapsed / 1000000.0
                       << " ms :" << _jit_obj_cache->get_func_name()
                       << " , mem cost: " << _jit_obj_cache->get_code_size();
            _jit_function = _jit_obj_cache->get_func();
            if (_jit_function == nullptr) {
                return Status::RuntimeError("JIT func must be not null");
            }
        }
    }
    return Status::OK();
}

StatusOr<ColumnPtr> JITExpr::evaluate_checked(starrocks::ExprContext* context, Chunk* ptr) {
    // If the expr fails to compile, evaluate using the original expr.
    if (UNLIKELY(_jit_function == nullptr)) {
        return _expr->evaluate_checked(context, ptr);
    }

    std::vector<JITColumn> jit_columns;
    jit_columns.reserve(_children.size() + 1);
    Columns args;
    args.reserve(_children.size() + 1);
    auto unfold_ptr = [&jit_columns](const ColumnPtr& column) {
        DCHECK(!column->is_constant());
        auto [un_col, un_col_null] = ColumnHelper::unpack_nullable_column(column);
        auto data_col_ptr = reinterpret_cast<const int8_t*>(un_col->raw_data());
        const int8_t* null_flags_ptr = nullptr;
        if (un_col_null != nullptr) {
            null_flags_ptr = reinterpret_cast<const int8_t*>(un_col_null->raw_data());
        }
        jit_columns.emplace_back(JITColumn{data_col_ptr, null_flags_ptr});
    };
    size_t num_rows = 0;
    for (Expr* child : _children) {
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(context, child, ptr);
        num_rows = std::max<size_t>(num_rows, column->size());
        args.emplace_back(column);
    }
    if (ptr != nullptr) {
        num_rows = ptr->num_rows();
    }
    auto result_column = ColumnHelper::create_column(type(), is_nullable(), false, num_rows);
    if (num_rows == 0) {
        return result_column;
    }
    Columns backup_args;
    backup_args.reserve(_children.size() + 1);
    for (auto i = 0; i < _children.size(); i++) {
        auto column = args[i];
        auto child = _children[i];
        if (UNLIKELY((column->is_constant() ^ child->is_constant()) ||
                     (column->is_nullable() ^ child->is_nullable()))) {
            VLOG_QUERY << "[JIT INPUT] expr const = " << child->is_constant() << " null= " << child->is_nullable()
                       << " but col const = " << column->is_constant() << " null = " << column->is_nullable()
                       << " expr= " << child->debug_string() << " col= " << column->get_name();
        }

        if (column->is_constant()) {
            column = ColumnHelper::unfold_const_column(child->type(), num_rows, column);
        }
        DCHECK(num_rows == column->size())
                << "size unequal " + std::to_string(num_rows) + " != " + std::to_string(column->size());

        if (child->is_nullable() && !column->is_nullable()) {
            column = NullableColumn::create(column, NullColumn::create(column->size(), 0));
        } else if (!child->is_nullable() && column->is_nullable()) {
            if (column->has_null()) {
                return Status::RuntimeError(
                        "[JIT] an expression comes out unexpected null values, please set jit_level = 0 to disable jit "
                        "and retry");
            }
        }
        unfold_ptr(column);
        backup_args.emplace_back(column);
    }

    unfold_ptr(result_column->as_mutable_ptr());
    // inputs are not empty.
    _jit_function(num_rows, jit_columns.data());
    //TODO: _jit_function return has_null
    if (is_nullable()) {
        down_cast<NullableColumn*>(result_column.get())->update_has_null();
    }
    return result_column;
}

} // namespace starrocks