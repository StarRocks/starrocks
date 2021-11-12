// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/function_call_expr.h"

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "exprs/anyval_util.h"
#include "exprs/vectorized/builtin_functions.h"
#include "gutil/strings/substitute.h"
#include "runtime/user_function_cache.h"

namespace starrocks::vectorized {

VectorizedFunctionCallExpr::VectorizedFunctionCallExpr(const TExprNode& node) : Expr(node), _fn_desc(nullptr) {}

Status VectorizedFunctionCallExpr::prepare(starrocks::RuntimeState* state, const starrocks::RowDescriptor& row_desc,
                                           starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, row_desc, context));

    if (!_fn.__isset.fid) {
        return Status::InternalError("Vectorized engine doesn't implement function " + _fn.name.function_name);
    }

    _fn_desc = BuiltinFunctions::find_builtin_function(_fn.fid);

    if (_fn_desc == nullptr || _fn_desc->scalar_function == nullptr) {
        return Status::InternalError("Vectorized engine doesn't implement function " + _fn.name.function_name);
    }

    if (_fn_desc->args_nums > _children.size()) {
        return Status::InternalError(strings::Substitute("Vectorized function $0 requires $1 arguments but given $2",
                                                         _fn.name.function_name, _fn_desc->args_nums,
                                                         _children.size()));
    }

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> args_types;

    for (Expr* child : _children) {
        args_types.push_back(AnyValUtil::column_type_to_type_desc(child->type()));
    }

    // todo: varargs use for allocate slice memory, need compute buffer size
    //  for varargs in vectorized engine?
    _fn_context_index = context->register_func(state, return_type, args_types, 0);

    _is_returning_random_value = _fn.fid == 10300 /* rand */ || _fn.fid == 10301 /* random */ ||
                                 _fn.fid == 10302 /* rand */ || _fn.fid == 10303 /* random */ ||
                                 _fn.fid == 100015 /* uuid */;

    return Status::OK();
}

Status VectorizedFunctionCallExpr::open(starrocks::RuntimeState* state, starrocks::ExprContext* context,
                                        FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        std::vector<ColumnPtr> const_columns;
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            const_columns.emplace_back(child->evaluate_const(context));
        }
        fn_ctx->impl()->set_constant_columns(std::move(const_columns));
    }

    if (_fn_desc->prepare_function != nullptr) {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
        }

        RETURN_IF_ERROR(_fn_desc->prepare_function(fn_ctx, FunctionContext::THREAD_LOCAL));
    }

    // Todo: We will use output_scale in the result_writer to format the
    //  output in row engine, but we need set output scale in vectorized engine?
    if (_fn.name.function_name == "round" && _type.type == TYPE_DOUBLE) {
        if (_children[1]->is_constant()) {
            ColumnPtr ptr = _children[1]->evaluate(context, nullptr);
            _output_scale =
                    std::static_pointer_cast<Int32Column>(std::static_pointer_cast<ConstColumn>(ptr)->data_column())
                            ->get_data()[0];
        }
    }

    return Status::OK();
}

void VectorizedFunctionCallExpr::close(starrocks::RuntimeState* state, starrocks::ExprContext* context,
                                       FunctionContext::FunctionStateScope scope) {
    if (_fn_desc != nullptr && _fn_desc->close_function != nullptr) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        _fn_desc->close_function(fn_ctx, FunctionContext::THREAD_LOCAL);

        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            _fn_desc->close_function(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
        }
    }

    Expr::close(state, context, scope);
}

bool VectorizedFunctionCallExpr::is_constant() const {
    if (_is_returning_random_value) {
        return false;
    }

    return Expr::is_constant();
}

ColumnPtr VectorizedFunctionCallExpr::evaluate(starrocks::ExprContext* context, vectorized::Chunk* ptr) {
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);

    Columns args;
    args.reserve(_children.size());
    for (Expr* child : _children) {
        ColumnPtr column = context->evaluate(child, ptr);
        args.emplace_back(column);
    }

    if (_is_returning_random_value) {
        if (ptr != nullptr) {
            args.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(ptr->num_rows(), ptr->num_rows()));
        } else {
            args.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(1, 1));
        }
    }

#ifndef NDEBUG
    if (ptr != nullptr) {
        size_t size = ptr->num_rows();
        // Ensure all columns have the same size
        for (const ColumnPtr& c : args) {
            CHECK_EQ(size, c->size());
        }
    }
#endif

    ColumnPtr result = _fn_desc->scalar_function(fn_ctx, args);
    // For no args function call (pi, e)
    if (result->is_constant() && ptr != nullptr) {
        result->resize(ptr->num_rows());
    }
    return result;
}

} // namespace starrocks::vectorized
