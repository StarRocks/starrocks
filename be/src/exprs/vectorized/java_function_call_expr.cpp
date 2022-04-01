// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/java_function_call_expr.h"

#include <algorithm>
#include <functional>
#include <future>
#include <memory>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

#include "column/column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/anyval_util.h"
#include "exprs/vectorized/java_function_call_expr.h"
#include "fmt/compile.h"
#include "fmt/core.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/casts.h"
#include "jni.h"
#include "jni_md.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "runtime/user_function_cache.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "udf/java/utils.h"
#include "udf/udf.h"
#include "util/slice.h"
#include "util/unaligned_access.h"

#define APPLY_FOR_NUMBERIC_TYPE(M) \
    M(TYPE_TINYINT)                \
    M(TYPE_SMALLINT)               \
    M(TYPE_INT)                    \
    M(TYPE_BIGINT)                 \
    M(TYPE_FLOAT)                  \
    M(TYPE_DOUBLE)                 \
    M(TYPE_BOOLEAN)

namespace starrocks::vectorized {

struct UDFFunctionCallHelper {
    JavaUDFContext* fn_desc;
    JavaMethodDescriptor* call_desc;
    std::vector<std::string> _data_buffer;

    // Now we don't support primitive type function
    ColumnPtr call(FunctionContext* ctx, Columns& columns, size_t size) {
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        std::vector<DirectByteBuffer> buffers;
        std::vector<jobject> args;
        int num_cols = ctx->get_num_args();
        std::vector<const Column*> input_cols;

        for (int i = 0; i < columns.size(); ++i) {
            if (columns[i]->only_null()) {
                // we will handle NULL later
            } else if (columns[i]->is_constant()) {
                columns[i] = ColumnHelper::unpack_and_duplicate_const_column(size, columns[i]);
            }
        }

        for (auto col : columns) {
            input_cols.emplace_back(col.get());
        }
        // convert input columns to object columns
        std::vector<jobject> input_col_objs;
        JavaDataTypeConverter::convert_to_boxed_array(ctx, &buffers, input_cols.data(), num_cols, size,
                                                      &input_col_objs);
        // call UDF method
        jobject res = helper.batch_call(ctx, fn_desc->udf_handle, fn_desc->evaluate->method, input_col_objs.data(),
                                        input_col_objs.size(), size);

        env->PushLocalFrame(size * (num_cols + 1));
        // get result
        auto result_cols = get_boxed_result(res, size);
        // call clear
        env->PopLocalFrame(nullptr);
        for (auto ref : args) {
            env->DeleteLocalRef(ref);
        }
        env->DeleteLocalRef(res);

        return result_cols;
    }

#define GET_BOX_RESULT(NAME, cxx_type)                                           \
    case NAME: {                                                                 \
        auto null_col = NullColumn::create(num_rows);                            \
        auto data_col = RunTimeColumnType<NAME>::create(num_rows);               \
        auto& null_data = null_col->get_data();                                  \
        auto& container = data_col->get_data();                                  \
        for (int i = 0; i < num_rows; ++i) {                                     \
            auto data = env->GetObjectArrayElement((jobjectArray)result, i);     \
            if (data != nullptr) {                                               \
                container[i] = helper.val##cxx_type(data);                       \
            } else {                                                             \
                null_data[i] = true;                                             \
            }                                                                    \
        }                                                                        \
        return NullableColumn::create(std::move(data_col), std::move(null_col)); \
    }

    ColumnPtr get_boxed_result(jobject result, size_t num_rows) {
        if (result == nullptr) {
            return ColumnHelper::create_const_null_column(num_rows);
        }
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        DCHECK(call_desc->method_desc[0].is_box);
        switch (call_desc->method_desc[0].type) {
            GET_BOX_RESULT(TYPE_BOOLEAN, uint8_t)
            GET_BOX_RESULT(TYPE_TINYINT, int8_t)
            GET_BOX_RESULT(TYPE_SMALLINT, int16_t)
            GET_BOX_RESULT(TYPE_INT, int32_t)
            GET_BOX_RESULT(TYPE_BIGINT, int64_t)
            GET_BOX_RESULT(TYPE_FLOAT, float)
            GET_BOX_RESULT(TYPE_DOUBLE, double)
        case TYPE_VARCHAR: {
            _data_buffer.resize(num_rows);
            auto null_col = NullColumn::create(num_rows);
            auto& null_data = null_col->get_data();
            std::vector<Slice> slices;
            slices.resize(num_rows);
            for (int i = 0; i < num_rows; ++i) {
                auto data = env->GetObjectArrayElement((jobjectArray)result, i);
                if (data != nullptr) {
                    slices[i] = helper.sliceVal((jstring)data, &_data_buffer[i]);
                } else {
                    null_data[i] = true;
                }
            }
            auto data_col = BinaryColumn::create();
            data_col->append_strings(slices);
            return NullableColumn::create(std::move(data_col), std::move(null_col));
        }
        default:
            DCHECK(false) << "unsupport type:" << call_desc->method_desc[0].type;
        }
        return ColumnHelper::create_const_null_column(num_rows);
    }
};

JavaFunctionCallExpr::JavaFunctionCallExpr(const TExprNode& node) : Expr(node) {}

ColumnPtr JavaFunctionCallExpr::evaluate(ExprContext* context, vectorized::Chunk* ptr) {
    Columns columns(children().size());

    for (int i = 0; i < _children.size(); ++i) {
        columns[i] = _children[i]->evaluate(context, ptr);
    }

    return _call_helper->call(context->fn_context(_fn_context_index), columns, ptr != nullptr ? ptr->num_rows() : 1);
}

JavaFunctionCallExpr::~JavaFunctionCallExpr() {
    auto promise = call_function_in_pthread(_runtime_state, [this]() {
        this->_func_desc.reset();
        this->_call_helper.reset();
        return Status::OK();
    });
    promise->get_future().get();
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
    _fn_context_index = context->register_func(state, return_type, args_types, 0);
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

Status JavaFunctionCallExpr::open(RuntimeState* state, ExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    // init parent open
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    // init function context
    Columns const_columns;
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        const_columns.reserve(_children.size());
        for (const auto& child : _children) {
            const_columns.emplace_back(child->evaluate_const(context));
        }
    }
    auto open_state = [this, scope]() {
        // init class loader and analyzer
        std::string libpath;
        auto function_cache = UserFunctionCache::instance();
        RETURN_IF_ERROR(function_cache->get_libpath(_fn.fid, _fn.hdfs_location, _fn.checksum, &libpath));
        _func_desc->udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
        RETURN_IF_ERROR(_func_desc->udf_classloader->init());
        _func_desc->analyzer = std::make_unique<ClassAnalyzer>();
        _func_desc->udf_class = _func_desc->udf_classloader->getClass(_fn.scalar_fn.symbol);
        if (_func_desc->udf_class.clazz() == nullptr) {
            return Status::InternalError(fmt::format("Not found symbol:{}", _fn.scalar_fn.symbol));
        }

        auto add_method = [&](const std::string& name, std::unique_ptr<JavaMethodDescriptor>* res) {
            bool has_method = false;
            std::string method_name = name;
            std::string signature;
            std::vector<MethodTypeDescriptor> mtdesc;
            RETURN_IF_ERROR(_func_desc->analyzer->has_method(_func_desc->udf_class.clazz(), method_name, &has_method));
            if (has_method) {
                RETURN_IF_ERROR(
                        _func_desc->analyzer->get_signature(_func_desc->udf_class.clazz(), method_name, &signature));
                RETURN_IF_ERROR(_func_desc->analyzer->get_method_desc(signature, &mtdesc));
                *res = std::make_unique<JavaMethodDescriptor>();
                (*res)->name = std::move(method_name);
                (*res)->signature = std::move(signature);
                (*res)->method_desc = std::move(mtdesc);
                ASSIGN_OR_RETURN((*res)->method,
                                 _func_desc->analyzer->get_method_object(_func_desc->udf_class.clazz(), name));
            }
            return Status::OK();
        };

        // Now we don't support prepare/close for UDF
        // RETURN_IF_ERROR(add_method("prepare", &_func_desc->prepare));
        // RETURN_IF_ERROR(add_method("method_close", &_func_desc->close));
        RETURN_IF_ERROR(add_method("evaluate", &_func_desc->evaluate));

        // create UDF function instance
        RETURN_IF_ERROR(_func_desc->udf_class.newInstance(&_func_desc->udf_handle));

        _call_helper = std::make_shared<UDFFunctionCallHelper>();
        _call_helper->fn_desc = _func_desc.get();
        _call_helper->call_desc = _func_desc->evaluate.get();

        if (_func_desc->prepare != nullptr) {
            // we only support fragment local scope to call prepare
            if (scope == FunctionContext::FRAGMENT_LOCAL) {
                // TODO: handle prepare function
            }
        }
        return Status::OK();
    };
    RETURN_IF_ERROR(call_function_in_pthread(state, open_state)->get_future().get());
    return Status::OK();
}

void JavaFunctionCallExpr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    if (_func_desc && _func_desc->close) {
        // Now we only support FRAGMENT LOCAL scope close
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            _call_udf_close();
        }
    }
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        _func_desc.reset();
        _call_helper.reset();
    }
    Expr::close(state, context, scope);
}

void JavaFunctionCallExpr::_call_udf_close() {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    jmethodID methodID = env->GetMethodID(_func_desc->udf_class.clazz(), _func_desc->close->name.c_str(),
                                          _func_desc->close->signature.c_str());
    env->CallVoidMethod(_func_desc->udf_handle, methodID);
    if (jthrowable jthr = env->ExceptionOccurred(); jthr) {
        LOG(WARNING) << "Exception occur:" << helper.dumpExceptionString(jthr);
        env->ExceptionClear();
    }
}

} // namespace starrocks::vectorized