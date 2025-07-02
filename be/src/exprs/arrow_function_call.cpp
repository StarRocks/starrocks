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

#include "exprs/arrow_function_call.h"

#include <memory>
#include <mutex>

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exprs/function_context.h"
#include "gen_cpp/Types_types.h"
#include "runtime/current_thread.h"
#include "runtime/user_function_cache.h"
#include "udf/python/callstub.h"
#include "util/phmap/phmap.h"

namespace starrocks {

using InnerStateMap = phmap::parallel_flat_hash_map<int32_t, std::unique_ptr<UDFCallStub>, phmap::Hash<int32_t>,
                                                    phmap::EqualTo<int32_t>, phmap::Allocator<int32_t>,
                                                    NUM_LOCK_SHARD_LOG, std::mutex>;

struct ArrowCallStubCtx {
    InnerStateMap inner_states;
    std::mutex inner_states_lock;
};

ArrowFunctionCallExpr::ArrowFunctionCallExpr(const TExprNode& node) : Expr(node) {}

StatusOr<ColumnPtr> ArrowFunctionCallExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    Columns columns(children().size());
    size_t num_rows = chunk != nullptr ? chunk->num_rows() : 1;
    for (int i = 0; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(columns[i], _children[i]->evaluate_checked(context, chunk));
        columns[i] = ColumnHelper::unfold_const_column(_children[i]->type(), num_rows, columns[i]);
    }

    // get call stub
    int32_t driver_id = CurrentThread::current().get_driver_id();
    FunctionContext* function_context = context->fn_context(_fn_context_index);
    UDFCallStub* stub = nullptr;
    _call_stub_ctx->inner_states.lazy_emplace_l(
            driver_id, [&](auto& value) { stub = value.get(); },
            [&](auto build) {
                auto value = _build_stub(driver_id, function_context);
                stub = value.get();
                build(driver_id, std::move(value));
            });

    return stub->evaluate(columns, num_rows);
}

Status ArrowFunctionCallExpr::prepare(RuntimeState* state, ExprContext* context) {
    _runtime_state = state;
    // init Expr::prepare
    RETURN_IF_ERROR(Expr::prepare(state, context));
    DCHECK(_fn.__isset.fid);

    FunctionContext::TypeDesc return_type = _type;
    std::vector<FunctionContext::TypeDesc> args_types;

    for (Expr* child : _children) {
        args_types.push_back(child->type());
    }

    _fn_context_index = context->register_func(state, return_type, args_types);
    context->fn_context(_fn_context_index)->set_is_udf(true);

    return Status::OK();
}

Status ArrowFunctionCallExpr::open(RuntimeState* state, ExprContext* context,
                                   FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    Columns const_columns;
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ASSIGN_OR_RETURN(auto&& child_col, child->evaluate_const(context))
            const_columns.emplace_back(std::move(child_col));
        }
        fn_ctx->set_constant_columns(std::move(const_columns));
    }
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto function_cache = UserFunctionCache::instance();
        if (_fn.hdfs_location != "inline") {
            RETURN_IF_ERROR(function_cache->get_libpath(_fn.fid, _fn.hdfs_location, _fn.checksum, &_lib_path));
        } else {
            _lib_path = "inline";
        }
        _call_stub_ctx = std::make_shared<ArrowCallStubCtx>();
    }
    return Status::OK();
}

void ArrowFunctionCallExpr::close(RuntimeState* state, ExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    Expr::close(state, context, scope);
}

bool ArrowFunctionCallExpr::is_constant() const {
    return false;
}

std::unique_ptr<UDFCallStub> ArrowFunctionCallExpr::_build_stub(int32_t driver_id, FunctionContext* context) {
    auto binary_type = _fn.binary_type;
    if (binary_type == TFunctionBinaryType::PYTHON) {
        PyFunctionDescriptor py_func_desc;
        py_func_desc.symbol = _fn.scalar_fn.symbol;
        py_func_desc.location = _lib_path;
        py_func_desc.input_type = _fn.input_type;
        py_func_desc.input_types = context->get_arg_types();
        py_func_desc.return_type = context->get_return_type();
        py_func_desc.content = _fn.content;
        return build_py_call_stub(context, py_func_desc);
    }
    return create_error_call_stub(Status::NotFound(fmt::format("unsupported function type:{}", binary_type)));
}

} // namespace starrocks