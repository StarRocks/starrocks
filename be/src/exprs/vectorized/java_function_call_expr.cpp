// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/java_function_call_expr.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <sstream>
#include <vector>

#include "column/column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
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
#include "udf/java/java_udf.h"
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
    FunctionContext* fn_ctx;
    JavaUDFContext* fn_desc;
    JavaMethodDescriptor* call_desc;
    std::vector<std::string> _data_buffer;
    // Now we only support String/int
    ColumnPtr call(FunctionContext* ctxs, Columns& columns, size_t size) {
        auto& helper = JVMFunctionHelper::getInstance();

        size_t num_cols = columns.size();

        std::vector<std::vector<jvalue>> cast_values;
        cast_values.resize(num_cols);
        for (int i = 0; i < num_cols; ++i) {
            cast_values[i].resize(size);
        }

        // step 1
        // cast column to jvalue vector
        for (int i = 0; i < num_cols; ++i) {
            cast_type_to_jvalue(&cast_values[i], columns[i].get(), i, size);
        }

        // step 2
        // combine input jvalue to jvalue array
        std::vector<jvalue> params(num_cols * size);
        for (int i = 0; i < num_cols; ++i) {
            combine_inputs(&params, cast_values[i], columns[i].get(), i, num_cols, size);
        }
        // step 3
        // call evalute
        std::vector<jvalue> result(size);
        call_evalute(&result, params, num_cols, size);

        // step 4
        // get result
        auto res = get_result(result);

        // TODO: add error message to Context
        if (auto jthr = helper.getEnv()->ExceptionOccurred(); jthr != nullptr) {
            std::string err = fmt::format("execute UDF Function meet Exception:{}", helper.dumpExceptionString(jthr));
            LOG(WARNING) << err;
            ctxs->set_error(err.c_str());
            helper.getEnv()->ExceptionClear();
        }

        // step 5
        // do clear
        for (int i = 0; i < num_cols; ++i) {
            do_clear(&cast_values[i], call_desc->method_desc[i + 1]);
        }
        do_clear(&result, call_desc->method_desc[0]);

        return res;
    }

    void do_clear(std::vector<jvalue>* jvalues, const MethodTypeDescriptor& desc) {
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        if (desc.is_box) {
            int size = jvalues->size();
            for (int i = 0; i < size; ++i) {
                env->DeleteLocalRef((*jvalues)[i].l);
            }
        }
    }

    template <PrimitiveType TYPE>
    ColumnPtr get_primtive_result(const std::vector<jvalue>& result) {
        auto res = RunTimeColumnType<TYPE>::create(result.size());
        auto& container = res->get_data();
        for (int i = 0; i < result.size(); ++i) {
            unaligned_store<RunTimeCppType<TYPE>>(&container[i],
                                                  *reinterpret_cast<const RunTimeCppType<TYPE>*>(&result[i]));
        }
        return res;
    }

    ColumnPtr get_result(const std::vector<jvalue>& result) {
        auto& helper = JVMFunctionHelper::getInstance();
        if (!call_desc->method_desc[0].is_box) {
            switch (call_desc->method_desc[0].type) {
#define M(NAME)                                   \
    case NAME: {                                  \
        return get_primtive_result<NAME>(result); \
    }
                APPLY_FOR_NUMBERIC_TYPE(M)
#undef M
            default:
                DCHECK(false) << "Not support type";
                break;
            }
        } else {
#define GET_BOX_RESULT(NAME, cxx_type)                                           \
    case NAME: {                                                                 \
        auto null_col = NullColumn::create(result.size());                       \
        auto data_col = RunTimeColumnType<NAME>::create(result.size());          \
        auto& null_data = null_col->get_data();                                  \
        auto& container = data_col->get_data();                                  \
        for (int i = 0; i < result.size(); ++i) {                                \
            if (result[i].l != nullptr) {                                        \
                container[i] = helper.valint8_t(result[i].l);                    \
            } else {                                                             \
                null_data[i] = true;                                             \
            }                                                                    \
        }                                                                        \
        return NullableColumn::create(std::move(data_col), std::move(null_col)); \
    }
            // Now result was always nullable
            switch (call_desc->method_desc[0].type) {
                GET_BOX_RESULT(TYPE_BOOLEAN, uint8_t)
                GET_BOX_RESULT(TYPE_TINYINT, int8_t)
                GET_BOX_RESULT(TYPE_SMALLINT, int16_t)
                GET_BOX_RESULT(TYPE_INT, int32_t)
                GET_BOX_RESULT(TYPE_BIGINT, int64_t)
            case TYPE_VARCHAR: {
                _data_buffer.resize(result.size());
                auto null_col = NullColumn::create(result.size());
                auto& null_data = null_col->get_data();
                std::vector<Slice> slices;
                slices.resize(result.size());
                for (int i = 0; i < result.size(); ++i) {
                    if (result[i].l != nullptr) {
                        slices[i] = helper.sliceVal((jstring)result[i].l, &_data_buffer[i]);
                    } else {
                        null_data[i] = true;
                    }
                }
                auto data_col = BinaryColumn::create();
                data_col->append_strings(slices);
                return NullableColumn::create(std::move(data_col), std::move(null_col));
            }
            default:
                DCHECK(false) << "Not support type";
                break;
            }
        }
        return nullptr;
    }

    void call_evalute(std::vector<jvalue>* result, const std::vector<jvalue>& params, int num_cols, int num_rows) {
        auto& helper = JVMFunctionHelper::getInstance();
        JNIEnv* env = helper.getEnv();
        jmethodID methodID = env->GetMethodID(fn_desc->udf_class.clazz(), fn_desc->evaluate->name.c_str(),
                                              fn_desc->evaluate->sign.c_str());
        DCHECK(methodID != nullptr);
        if (!call_desc->method_desc[0].is_box) {
            switch (call_desc->method_desc[0].type) {
            case TYPE_BOOLEAN: {
                for (int i = 0, j = 0; i < num_rows; ++i, j += num_cols) {
                    (*result)[i].z = env->CallBooleanMethodA(fn_desc->udf_handle, methodID, &params[j]);
                }
                break;
            }
            case TYPE_TINYINT: {
                for (int i = 0, j = 0; i < num_rows; ++i, j += num_cols) {
                    (*result)[i].b = env->CallByteMethodA(fn_desc->udf_handle, methodID, &params[j]);
                }
                break;
            }
            case TYPE_SMALLINT: {
                for (int i = 0, j = 0; i < num_rows; ++i, j += num_cols) {
                    (*result)[i].s = env->CallShortMethodA(fn_desc->udf_handle, methodID, &params[j]);
                }
                break;
            }
            case TYPE_INT: {
                for (int i = 0, j = 0; i < num_rows; ++i, j += num_cols) {
                    (*result)[i].i = env->CallIntMethodA(fn_desc->udf_handle, methodID, &params[j]);
                }
                break;
            }
            case TYPE_BIGINT: {
                for (int i = 0, j = 0; i < num_rows; ++i, j += num_cols) {
                    (*result)[i].j = env->CallLongMethodA(fn_desc->udf_handle, methodID, &params[j]);
                }
                break;
            }
            default:
                DCHECK(false) << "Java UDF Not Support Type" << call_desc->method_desc[0].type;
                break;
            }

        } else {
            for (int i = 0, j = 0; i < num_rows; ++i, j += num_cols) {
                (*result)[i].l = env->CallObjectMethodA(fn_desc->udf_handle, methodID, &params[j]);
            }
        }
    }

    void combine_inputs(std::vector<jvalue>* res, const std::vector<jvalue>& data, Column* col, int col_idx,
                        int num_cols, int num_rows) {
        if (col->is_nullable()) {
            auto* null_col = down_cast<NullableColumn*>(col);
            const NullData& null_data = null_col->immutable_null_column_data();
            for (int i = 0, j = col_idx; i < num_rows; ++i, j += num_cols) {
                if (null_data[i]) {
                    (*res)[j].l = nullptr;
                } else {
                    (*res)[j] = data[i];
                }
            }
        } else {
            for (int i = 0, j = col_idx; i < num_rows; ++i, j += num_cols) {
                (*res)[j] = data[i];
            }
        }
    }

    // cast type to jvalue
    void cast_type_to_jvalue(std::vector<jvalue>* res, Column* column, int col_idx, size_t num_rows) {
        // handle nullable func
        switch (call_desc->method_desc[col_idx + 1].type) {
#define M(NAME)                                                                                   \
    case NAME: {                                                                                  \
        do_cast_type_to_jvalue<NAME>(res, call_desc->method_desc[col_idx + 1], column, num_rows); \
        break;                                                                                    \
    }
            APPLY_FOR_NUMBERIC_TYPE(M)
#undef M
        case TYPE_VARCHAR:
            do_cast_type_to_jvalue<TYPE_VARCHAR>(res, call_desc->method_desc[col_idx + 1], column, num_rows);
            break;
        default:
            break;
        }
    }

    template <PrimitiveType TYPE>
    jvalue transfer(RunTimeCppType<TYPE> data_value, JVMFunctionHelper& helper);

    template <PrimitiveType TYPE>
    void do_cast_type_to_jvalue(std::vector<jvalue>* res, const MethodTypeDescriptor& desc, Column* column, int size) {
        auto& res_data = *res;
        auto& helper = JVMFunctionHelper::getInstance();

        auto do_cast = [&](const RunTimeColumnType<TYPE>* spec_col, int size) {
            const auto& container = spec_col->get_data();
            if (desc.is_box) {
                // has null
                // not has null
                for (int i = 0; i < size; i++) {
                    res_data[i] = transfer<TYPE>(container[i], helper);
                }
            } else {
                for (int i = 0; i < size; i++) {
                    unaligned_store<RunTimeCppType<TYPE>>(&res_data[i], container[i]);
                }
            }
        };

        if (column->only_null()) {
            for (int i = 0; i < size; ++i) {
                memset(res->data(), 0, sizeof(jvalue));
            }
            return;
        }

        ColumnPtr guard;
        if (column->is_constant()) {
            auto data_col = ColumnHelper::get_data_column(column);
            const auto* spec_col = down_cast<RunTimeColumnType<TYPE>*>(data_col);
            do_cast(spec_col, 1);
            for (int i = 1; i < size; ++i) {
                res_data[i] = res_data[0];
            }
            return;
        }

        if (column->is_nullable()) {
            NullableColumn* nullable_col = down_cast<NullableColumn*>(column);
            const auto& spec_col = down_cast<RunTimeColumnType<TYPE>*>(nullable_col->data_column().get());
            // TODO:
            do_cast(spec_col, size);
        } else {
            const auto* spec_col = down_cast<RunTimeColumnType<TYPE>*>(column);
            do_cast(spec_col, size);
        }
    }
};

#define DEFINE_TRANSTER(TYPE, APPLY_FUNC)                                                                       \
    template <>                                                                                                 \
    jvalue UDFFunctionCallHelper::transfer<TYPE>(RunTimeCppType<TYPE> data_value, JVMFunctionHelper & helper) { \
        return {.l = APPLY_FUNC};                                                                               \
    }

DEFINE_TRANSTER(TYPE_BOOLEAN, helper.newBoolean(data_value));
DEFINE_TRANSTER(TYPE_TINYINT, helper.newByte(data_value));
DEFINE_TRANSTER(TYPE_SMALLINT, helper.newShort(data_value));
DEFINE_TRANSTER(TYPE_INT, helper.newInteger(data_value));
DEFINE_TRANSTER(TYPE_FLOAT, helper.newFloat(data_value));
DEFINE_TRANSTER(TYPE_BIGINT, helper.newLong(data_value));
DEFINE_TRANSTER(TYPE_DOUBLE, helper.newDouble(data_value));
DEFINE_TRANSTER(TYPE_VARCHAR, helper.newString(data_value.get_data(), data_value.get_size()));

JavaFunctionCallExpr::JavaFunctionCallExpr(const TExprNode& node) : Expr(node) {}

ColumnPtr JavaFunctionCallExpr::evaluate(ExprContext* context, vectorized::Chunk* ptr) {
    Columns columns(children().size());

    for (int i = 0; i < _children.size(); ++i) {
        columns[i] = _children[i]->evaluate(context, ptr);
    }

    return _call_helper->call(context->fn_context(_fn_context_index), columns, ptr != nullptr ? ptr->num_rows() : 1);
}

JavaFunctionCallExpr::~JavaFunctionCallExpr() = default;

// TODO support prepare
Status JavaFunctionCallExpr::prepare(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* context) {
    // init Expr::prepare
    RETURN_IF_ERROR(Expr::prepare(state, row_desc, context));

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

    // init class loader and analyzer
    std::string libpath;
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(_fn.id, _fn.hdfs_location, _fn.checksum, &libpath));
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
        std::string sign;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(_func_desc->analyzer->has_method(_func_desc->udf_class.clazz(), method_name, &has_method));
        if (has_method) {
            RETURN_IF_ERROR(_func_desc->analyzer->get_sign(_func_desc->udf_class.clazz(), method_name, &sign));
            RETURN_IF_ERROR(_func_desc->analyzer->get_method_desc(sign, &mtdesc));
            *res = std::make_unique<JavaMethodDescriptor>();
            (*res)->name = std::move(method_name);
            (*res)->sign = std::move(sign);
            (*res)->method_desc = std::move(mtdesc);
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
    _call_helper->fn_ctx = context->fn_context(_fn_context_index);
    _call_helper->fn_desc = _func_desc.get();
    _call_helper->call_desc = _func_desc->evaluate.get();

    if (_func_desc->prepare != nullptr) {
        // we only support fragment local scope to call prepare
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            // TODO: handle prepare function
        }
    }

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
                                          _func_desc->close->sign.c_str());
    env->CallVoidMethod(_func_desc->udf_handle, methodID);
    if (jthrowable jthr = env->ExceptionOccurred(); jthr) {
        LOG(WARNING) << "Exception occur:" << helper.dumpExceptionString(jthr);
        env->ExceptionClear();
    }
}

} // namespace starrocks::vectorized