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

#include "exprs/java_function_call_expr.h"

#include <any>
#include <memory>
#include <sstream>
#include <tuple>
#include <vector>

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/anyval_util.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "jni.h"
#include "runtime/types.h"
#include "runtime/user_function_cache.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "udf/java/utils.h"
#include "util/defer_op.h"

namespace starrocks {

struct UDFFunctionCallHelper {
    JavaUDFContext* fn_desc;
    JavaMethodDescriptor* call_desc;

    // Now we don't support logical type function
    ColumnPtr call(FunctionContext* ctx, Columns& columns, size_t size) {
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        std::vector<DirectByteBuffer> buffers;
        int num_cols = ctx->get_num_args();
        std::vector<const Column*> input_cols;

        for (auto& column : columns) {
            if (column->only_null()) {
                // we will handle NULL later
            } else if (column->is_constant()) {
                column = ColumnHelper::unpack_and_duplicate_const_column(size, column);
            }
        }

        for (const auto& col : columns) {
            input_cols.emplace_back(col.get());
        }
        // each input arguments as three local references (nullcolumn, offsetcolumn, bytescolumn)
        // result column as a ref
        env->PushLocalFrame((num_cols + 1) * 3 + 1);
        auto defer = DeferOp([env]() { env->PopLocalFrame(nullptr); });
        // convert input columns to object columns
        std::vector<jobject> input_col_objs;
        auto st = JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, input_cols.data(), num_cols, size,
                                                                &input_col_objs);
        RETURN_IF_UNLIKELY(!st.ok(), ColumnHelper::create_const_null_column(size));

        // call UDF method
        jobject res = helper.batch_call(fn_desc->call_stub.get(), input_col_objs.data(), input_col_objs.size(), size);
        RETURN_IF_UNLIKELY_NULL(res, ColumnHelper::create_const_null_column(size));
        // get result
        auto result_cols = get_boxed_result(ctx, res, size);
        return result_cols;
    }

    ColumnPtr get_boxed_result(FunctionContext* ctx, jobject result, size_t num_rows) {
        if (result == nullptr) {
            return ColumnHelper::create_const_null_column(num_rows);
        }
        auto& helper = JVMFunctionHelper::getInstance();
        DCHECK(call_desc->method_desc[0].is_box);
        TypeDescriptor type_desc(call_desc->method_desc[0].type);
        auto res = ColumnHelper::create_column(type_desc, true);
        helper.get_result_from_boxed_array(ctx, type_desc.type, res.get(), result, num_rows);
        down_cast<NullableColumn*>(res.get())->update_has_null();
        return res;
    }
};

JavaFunctionCallExpr::JavaFunctionCallExpr(const TExprNode& node) : Expr(node) {}

StatusOr<ColumnPtr> JavaFunctionCallExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    Columns columns(children().size());

    for (int i = 0; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(columns[i], _children[i]->evaluate_checked(context, ptr));
    }
    ColumnPtr res;
    auto call_udf = [&]() {
        res = _call_helper->call(context->fn_context(_fn_context_index), columns, ptr != nullptr ? ptr->num_rows() : 1);
        return Status::OK();
    };
    (void)call_function_in_pthread(_runtime_state, call_udf)->get_future().get();
    return res;
}

JavaFunctionCallExpr::~JavaFunctionCallExpr() {
    // nothing to do if JavaFunctionCallExpr has not been prepared
    if (_runtime_state == nullptr) return;
    auto promise = call_function_in_pthread(_runtime_state, [this]() {
        this->_func_desc.reset();
        this->_call_helper.reset();
        return Status::OK();
    });
    (void)promise->get_future().get();
}

// TODO support prepare UDF
Status JavaFunctionCallExpr::prepare(RuntimeState* state, ExprContext* context) {
    _runtime_state = state;
    // init Expr::prepare
    RETURN_IF_ERROR(Expr::prepare(state, context));

    if (!_fn.__isset.fid) {
        return Status::InternalError("Not Found function id for " + _fn.name.function_name);
    }

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> args_types;

    for (Expr* child : _children) {
        args_types.push_back(AnyValUtil::column_type_to_type_desc(child->type()));
    }

    // todo: varargs use for allocate slice memory, need compute buffer size
    //  for varargs in vectorized engine?
    _fn_context_index = context->register_func(state, return_type, args_types);
    context->fn_context(_fn_context_index)->set_is_udf(true);

    _func_desc = std::make_shared<JavaUDFContext>();
    // TODO:
    _is_returning_random_value = false;
    return Status::OK();
}

bool JavaFunctionCallExpr::is_constant() const {
    if (_is_returning_random_value) {
        return false;
    }
    return Expr::is_constant();
}

StatusOr<std::shared_ptr<JavaUDFContext>> JavaFunctionCallExpr::_build_udf_func_desc(
        ExprContext* context, FunctionContext::FunctionStateScope scope, const std::string& libpath) {
    auto desc = std::make_shared<JavaUDFContext>();
    // init class loader and analyzer
    desc->udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
    RETURN_IF_ERROR(desc->udf_classloader->init());
    desc->analyzer = std::make_unique<ClassAnalyzer>();

    ASSIGN_OR_RETURN(desc->udf_class, desc->udf_classloader->getClass(_fn.scalar_fn.symbol));

    auto add_method = [&](const std::string& name, std::unique_ptr<JavaMethodDescriptor>* res) {
        bool has_method = false;
        std::string method_name = name;
        std::string signature;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(desc->analyzer->has_method(desc->udf_class.clazz(), method_name, &has_method));
        if (has_method) {
            RETURN_IF_ERROR(desc->analyzer->get_signature(desc->udf_class.clazz(), method_name, &signature));
            RETURN_IF_ERROR(desc->analyzer->get_method_desc(signature, &mtdesc));
            *res = std::make_unique<JavaMethodDescriptor>();
            (*res)->name = std::move(method_name);
            (*res)->signature = std::move(signature);
            (*res)->method_desc = std::move(mtdesc);
            ASSIGN_OR_RETURN((*res)->method, desc->analyzer->get_method_object(desc->udf_class.clazz(), name));
        }
        return Status::OK();
    };

    // Now we don't support prepare/close for UDF
    // RETURN_IF_ERROR(add_method("prepare", &desc->prepare));
    // RETURN_IF_ERROR(add_method("method_close", &desc->close));
    RETURN_IF_ERROR(add_method("evaluate", &desc->evaluate));

    // create UDF function instance
    ASSIGN_OR_RETURN(desc->udf_handle, desc->udf_class.newInstance());
    // BatchEvaluateStub
    auto* stub_clazz = BatchEvaluateStub::stub_clazz_name;
    auto* stub_method_name = BatchEvaluateStub::batch_evaluate_method_name;
    auto udf_clazz = desc->udf_class.clazz();
    auto update_method = desc->evaluate->method.handle();

    ASSIGN_OR_RETURN(auto update_stub_clazz, desc->udf_classloader->genCallStub(stub_clazz, udf_clazz, update_method,
                                                                                ClassLoader::BATCH_EVALUATE));
    ASSIGN_OR_RETURN(auto method, desc->analyzer->get_method_object(update_stub_clazz.clazz(), stub_method_name));
    auto function_ctx = context->fn_context(_fn_context_index);
    desc->call_stub = std::make_unique<BatchEvaluateStub>(
            function_ctx, desc->udf_handle.handle(), std::move(update_stub_clazz), JavaGlobalRef(std::move(method)));

    if (desc->prepare != nullptr) {
        // we only support fragment local scope to call prepare
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            // TODO: handle prepare function
        }
    }

    return desc;
}

Status JavaFunctionCallExpr::open(RuntimeState* state, ExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    // init parent open
    RETURN_IF_ERROR(Expr::open(state, context, scope));
    RETURN_IF_ERROR(detect_java_runtime());
    // init function context
    Columns const_columns;
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ASSIGN_OR_RETURN(auto&& child_col, child->evaluate_const(context))
            const_columns.emplace_back(std::move(child_col));
        }
    }
    // cacheable
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto get_func_desc = [this, scope, context, state](const std::string& lib) -> StatusOr<std::any> {
            std::any func_desc;
            auto call = [&]() {
                ASSIGN_OR_RETURN(func_desc, _build_udf_func_desc(context, scope, lib));
                return Status::OK();
            };
            RETURN_IF_ERROR(call_function_in_pthread(state, call)->get_future().get());
            return func_desc;
        };

        auto function_cache = UserFunctionCache::instance();
        if (_fn.__isset.isolated && !_fn.isolated) {
            ASSIGN_OR_RETURN(auto desc, function_cache->load_cacheable_java_udf(_fn.fid, _fn.hdfs_location,
                                                                                _fn.checksum, get_func_desc));
            _func_desc = std::any_cast<std::shared_ptr<JavaUDFContext>>(desc);
        } else {
            std::string libpath;
            RETURN_IF_ERROR(function_cache->get_libpath(_fn.fid, _fn.hdfs_location, _fn.checksum, &libpath));
            ASSIGN_OR_RETURN(auto desc, get_func_desc(libpath));
            _func_desc = std::any_cast<std::shared_ptr<JavaUDFContext>>(desc);
        }

        _call_helper = std::make_shared<UDFFunctionCallHelper>();
        _call_helper->fn_desc = _func_desc.get();
        _call_helper->call_desc = _func_desc->evaluate.get();
    }
    return Status::OK();
}

void JavaFunctionCallExpr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    auto function_close = [this, scope]() {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            if (_func_desc && _func_desc->close) {
                _call_udf_close();
            }
            _func_desc.reset();
            _call_helper.reset();
        }
        return Status::OK();
    };
    (void)call_function_in_pthread(state, function_close)->get_future().get();
    Expr::close(state, context, scope);
}

void JavaFunctionCallExpr::_call_udf_close() {}

} // namespace starrocks
