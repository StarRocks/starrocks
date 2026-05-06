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
#include <functional>
#include <memory>
#include <sstream>
#include <tuple>
#include <vector>

#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "jni.h"
#include "runtime/user_function_cache.h"
#include "types/type_descriptor.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "udf/java/utils.h"

namespace starrocks {

struct UDFFunctionCallHelper {
    JavaUDFContext* fn_desc;
    JavaMethodDescriptor* call_desc;

    StatusOr<ColumnPtr> call(FunctionContext* ctx, Columns& columns, size_t size) {
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        int num_cols = ctx->get_num_args();
        std::vector<const Column*> input_cols;

        for (const auto& col : columns) {
            input_cols.emplace_back(col.get());
        }
        // each input arguments as three local references (nullcolumn, offsetcolumn, bytescolumn)
        // result column as a ref
        env->PushLocalFrame((num_cols + 1) * 3 + 1);
        auto defer = DeferOp([env]() { env->PopLocalFrame(nullptr); });

        // Pass the per-arg UdfTypeDesc jobjects cached on the UDF context. Only args
        // whose SQL type subtree contains a STRUCT carry a non-null desc; other
        // entries are null and the boxer falls back to JavaArrayConverter for those
        // subtrees.
        std::vector<jobject> arg_type_descs;
        arg_type_descs.reserve(fn_desc->evaluate_arg_type_descs.size());
        for (const auto& gref : fn_desc->evaluate_arg_type_descs) {
            arg_type_descs.emplace_back(gref.handle());
        }

        std::vector<jobject> input_col_objs;
        auto st = JavaDataTypeConverter::convert_to_boxed_array(ctx, input_cols.data(), num_cols, size, &input_col_objs,
                                                                &arg_type_descs);
        RETURN_IF_ERROR(st);

        // call UDF method
        ASSIGN_OR_RETURN(auto res, helper.batch_call(fn_desc->call_stub.get(), input_col_objs.data(),
                                                     input_col_objs.size(), size));
        // get result
        auto result_cols = get_boxed_result(ctx, res, size);
        return result_cols;
    }

    StatusOr<ColumnPtr> get_boxed_result(FunctionContext* ctx, jobject result, size_t num_rows) {
        if (result == nullptr) {
            return ColumnHelper::create_const_null_column(num_rows);
        }
        auto& helper = JVMFunctionHelper::getInstance();
        DCHECK(call_desc->method_desc[0].is_box);
        const auto& return_type = ctx->get_return_type();
        auto res = ColumnHelper::create_column(return_type, true);

        jobject return_desc = fn_desc->evaluate_return_type_desc.handle();
        if (return_desc != nullptr) {
            // Return type subtree contains a STRUCT (top-level, inside ARRAY, or
            // inside MAP). Hand off to the unified Java writeResult, which walks
            // the UdfTypeDesc tree and recursively drains records / lists / maps
            // / scalars into the native column tree.
            RETURN_IF_ERROR(helper.write_result(result, static_cast<int>(num_rows), reinterpret_cast<jlong>(res.get()),
                                                return_desc, ctx->error_if_overflow()));
        } else {
            // Plain scalar / DECIMAL / ARRAY / MAP without STRUCT: unified writer
            // dispatches DECIMAL internally based on the LogicalType.
            RETURN_IF_ERROR(helper.get_result_from_boxed_array(return_type.type, res.get(), result, num_rows,
                                                               return_type.precision, return_type.scale,
                                                               ctx->error_if_overflow()));
        }
        RETURN_IF_ERROR(ColumnHelper::update_nested_has_null(res.get()));
        return res;
    }
};

JavaFunctionCallExpr::JavaFunctionCallExpr(const TExprNode& node) : Expr(node) {}

StatusOr<ColumnPtr> JavaFunctionCallExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    Columns columns(children().size());

    for (int i = 0; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(columns[i], _children[i]->evaluate_checked(context, ptr));
    }
    StatusOr<ColumnPtr> res;
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

    FunctionContext::TypeDesc return_type = _type;
    std::vector<FunctionContext::TypeDesc> args_types;

    args_types.reserve(_children.size());
    for (Expr* child : _children) {
        args_types.push_back(child->type());
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
        FunctionContext::FunctionStateScope scope, const std::string& libpath) {
    auto desc = std::make_shared<JavaUDFContext>();
    // init class loader and analyzer
    desc->udf_classloader = std::make_unique<ClassLoader>(libpath);
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

    // Build a com.starrocks.udf.UdfTypeDesc Java object for each UDF argument and
    // for the return type whose SQL type subtree contains a STRUCT, walking
    // Method.getGenericParameterTypes / getGenericReturnType in lockstep with the
    // SQL type tree. The same UdfTypeDesc tree is the single source of type info
    // shared with both the input boxing path (via JNI field accessors) and the
    // unified Java writeResult helper.
    {
        auto type_subtree_has_struct = [](const TypeDescriptor& td) {
            std::function<bool(const TypeDescriptor&)> walk = [&](const TypeDescriptor& t) {
                if (t.type == TYPE_STRUCT) {
                    return true;
                }
                for (const auto& c : t.children) {
                    if (walk(c)) {
                        return true;
                    }
                }
                return false;
            };
            return walk(td);
        };

        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        jobject method_obj = desc->evaluate->method.handle();

        int num_args = static_cast<int>(_children.size());
        // JavaGlobalRef is move-only, so vector::resize(n, value) is unavailable.
        // Pre-fill with null-handle entries; STRUCT-bearing slots overwrite via move below.
        desc->evaluate_arg_type_descs.reserve(num_args);
        for (int i = 0; i < num_args; ++i) {
            desc->evaluate_arg_type_descs.emplace_back(nullptr);
        }

        bool any_struct = type_subtree_has_struct(_type);
        for (int i = 0; i < num_args && !any_struct; ++i) {
            if (type_subtree_has_struct(_children[i]->type())) {
                any_struct = true;
            }
        }

        if (any_struct) {
            jclass method_class = env->FindClass("java/lang/reflect/Method");
            DCHECK(method_class != nullptr);
            DeferOp drop_method_class([&]() { env->DeleteLocalRef(method_class); });

            jmethodID get_generic_param =
                    env->GetMethodID(method_class, "getGenericParameterTypes", "()[Ljava/lang/reflect/Type;");
            jmethodID get_generic_return =
                    env->GetMethodID(method_class, "getGenericReturnType", "()Ljava/lang/reflect/Type;");
            jmethodID is_var_args_mid = env->GetMethodID(method_class, "isVarArgs", "()Z");
            jmethodID get_param_count_mid = env->GetMethodID(method_class, "getParameterCount", "()I");
            DCHECK(get_generic_param != nullptr && get_generic_return != nullptr);
            DCHECK(is_var_args_mid != nullptr && get_param_count_mid != nullptr);

            jobjectArray generic_params = (jobjectArray)env->CallObjectMethod(method_obj, get_generic_param);
            if (env->ExceptionCheck() || generic_params == nullptr) {
                env->ExceptionClear();
                return Status::InternalError("failed to introspect UDF evaluate generic parameter types");
            }
            DeferOp drop_generic_params([&]() { env->DeleteLocalRef(generic_params); });

            // Varargs handling: when the Java method declares `T... vs`, reflection exposes
            // the varargs slot's formal type as either Class<T[]> (raw array) or
            // GenericArrayType(List<Inner>) for parameterized element types. The expanded SQL
            // arg list (`_children`) has one entry per actual call-site value, exceeding the
            // Java parameter count. For SQL args at index >= num_fixed_params we resolve
            // against the varargs ELEMENT type, unwrapping the array layer once.
            jboolean is_varargs = env->CallBooleanMethod(method_obj, is_var_args_mid);
            jint java_param_count = env->CallIntMethod(method_obj, get_param_count_mid);
            int num_fixed_params = is_varargs ? std::max(0, java_param_count - 1) : java_param_count;

            // Resolve the varargs element type once if the varargs slot is reachable from any
            // STRUCT-bearing SQL arg. The result is a fresh local ref the caller deletes.
            jobject varargs_elem_formal = nullptr;
            DeferOp drop_varargs_elem([&]() {
                if (varargs_elem_formal) env->DeleteLocalRef(varargs_elem_formal);
            });
            if (is_varargs && num_args > num_fixed_params) {
                jobject varargs_formal = env->GetObjectArrayElement(generic_params, num_fixed_params);
                if (env->ExceptionCheck() || varargs_formal == nullptr) {
                    env->ExceptionClear();
                    return Status::InternalError("failed to read UDF varargs formal type");
                }
                DeferOp drop_varargs_formal([&]() { env->DeleteLocalRef(varargs_formal); });

                jclass gat_clazz = env->FindClass("java/lang/reflect/GenericArrayType");
                DeferOp drop_gat_clazz([&]() {
                    if (gat_clazz) env->DeleteLocalRef(gat_clazz);
                });
                jclass class_clazz = env->FindClass("java/lang/Class");
                DeferOp drop_class_clazz([&]() {
                    if (class_clazz) env->DeleteLocalRef(class_clazz);
                });

                if (gat_clazz != nullptr && env->IsInstanceOf(varargs_formal, gat_clazz)) {
                    // List<Inner>[] / Map<K,V>[] → ParameterizedType element.
                    jmethodID get_component =
                            env->GetMethodID(gat_clazz, "getGenericComponentType", "()Ljava/lang/reflect/Type;");
                    varargs_elem_formal = env->CallObjectMethod(varargs_formal, get_component);
                } else if (class_clazz != nullptr && env->IsInstanceOf(varargs_formal, class_clazz)) {
                    // Raw array Class — e.g. Rec[] for Rec... varargs. Drop the array layer
                    // via Class.getComponentType().
                    jmethodID get_component = env->GetMethodID(class_clazz, "getComponentType", "()Ljava/lang/Class;");
                    varargs_elem_formal = env->CallObjectMethod(varargs_formal, get_component);
                } else {
                    return Status::InternalError("UDF varargs formal type is neither Class nor GenericArrayType");
                }
                if (env->ExceptionCheck() || varargs_elem_formal == nullptr) {
                    env->ExceptionClear();
                    return Status::InternalError("failed to unwrap UDF varargs element type");
                }
            }

            for (int i = 0; i < num_args; ++i) {
                if (!type_subtree_has_struct(_children[i]->type())) {
                    continue;
                }
                jobject formal = nullptr;
                DeferOp drop_formal([&]() {
                    if (formal) env->DeleteLocalRef(formal);
                });
                if (i < num_fixed_params) {
                    formal = env->GetObjectArrayElement(generic_params, i);
                    if (env->ExceptionCheck() || formal == nullptr) {
                        env->ExceptionClear();
                        return Status::InternalError(fmt::format("UDF evaluate parameter {} formal type is null", i));
                    }
                } else {
                    // Varargs slot — use the unwrapped element type. Bump its ref count so
                    // the per-iteration drop_formal can DeleteLocalRef without invalidating
                    // varargs_elem_formal for the next iteration.
                    formal = env->NewLocalRef(varargs_elem_formal);
                    if (formal == nullptr) {
                        return Status::InternalError("failed to retain UDF varargs element formal type");
                    }
                }
                ASSIGN_OR_RETURN(jobject local_desc, build_udf_type_desc(env, _children[i]->type(), formal));
                desc->evaluate_arg_type_descs[i] = JavaGlobalRef(env->NewGlobalRef(local_desc));
                env->DeleteLocalRef(local_desc);
            }

            if (type_subtree_has_struct(_type)) {
                jobject ret_formal = env->CallObjectMethod(method_obj, get_generic_return);
                if (env->ExceptionCheck() || ret_formal == nullptr) {
                    env->ExceptionClear();
                    return Status::InternalError("failed to introspect UDF evaluate generic return type");
                }
                DeferOp drop_ret([&]() { env->DeleteLocalRef(ret_formal); });
                ASSIGN_OR_RETURN(jobject local_ret_desc, build_udf_type_desc(env, _type, ret_formal));
                desc->evaluate_return_type_desc = JavaGlobalRef(env->NewGlobalRef(local_ret_desc));
                env->DeleteLocalRef(local_ret_desc);
            }
        }
    }

    // create UDF function instance
    ASSIGN_OR_RETURN(desc->udf_handle, desc->udf_class.newInstance());
    // BatchEvaluateStub
    auto* stub_clazz = BatchEvaluateStub::stub_clazz_name;
    auto* stub_method_name = BatchEvaluateStub::batch_evaluate_method_name;
    auto udf_clazz = desc->udf_class.clazz();
    auto update_method = desc->evaluate->method.handle();

    // For varargs UDFs, pass the actual number of varargs input columns (excluding fixed params)
    // so that the stub generator produces the correct signature.
    // method_desc layout: [return, fixedParam1, ..., fixedParamF, varargs_elem] → size = F + 2
    // so numFixedParams = method_desc.size() - 2.
    int num_fixed_params = (_fn.has_var_args && desc->evaluate)
                                   ? std::max(0, static_cast<int>(desc->evaluate->method_desc.size()) - 2)
                                   : 0;
    int num_actual_var_args = _fn.has_var_args ? std::max(0, static_cast<int>(_children.size()) - num_fixed_params) : 0;
    ASSIGN_OR_RETURN(auto update_stub_clazz,
                     desc->udf_classloader->genCallStub(stub_clazz, udf_clazz, update_method,
                                                        ClassLoader::BATCH_EVALUATE, num_actual_var_args));
    ASSIGN_OR_RETURN(auto method, desc->analyzer->get_method_object(update_stub_clazz.clazz(), stub_method_name));
    desc->call_stub = std::make_unique<BatchEvaluateStub>(desc->udf_handle.handle(), std::move(update_stub_clazz),
                                                          JavaGlobalRef(method));

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

    UserFunctionCache::FunctionCacheDesc func_cache_desc(_fn.fid, _fn.hdfs_location, _fn.checksum,
                                                         TFunctionBinaryType::SRJAR, _fn.cloud_configuration);
    // cacheable
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto get_func_desc = [this, scope, state](const std::string& lib) -> StatusOr<std::any> {
            std::any func_desc;
            auto call = [&]() {
                ASSIGN_OR_RETURN(func_desc, _build_udf_func_desc(scope, lib));
                return Status::OK();
            };
            RETURN_IF_ERROR(call_function_in_pthread(state, call)->get_future().get());
            return func_desc;
        };

        auto function_cache = UserFunctionCache::instance();
        if (_fn.__isset.isolated && !_fn.isolated) {
            ASSIGN_OR_RETURN(auto desc, function_cache->load_cacheable_java_udf(func_cache_desc, get_func_desc));
            _func_desc = std::any_cast<std::shared_ptr<JavaUDFContext>>(desc.second);
        } else {
            std::string libpath;
            RETURN_IF_ERROR(function_cache->get_libpath(func_cache_desc, &libpath));
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
