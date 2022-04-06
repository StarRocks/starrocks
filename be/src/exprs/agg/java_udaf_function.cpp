// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/agg/java_udaf_function.h"

#include <memory>

#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gutil/casts.h"
#include "jni.h"
#include "runtime/primitive_type.h"
#include "runtime/user_function_cache.h"

#define APPLY_FOR_NUMBERIC_TYPE(M) \
    M(TYPE_BOOLEAN)                \
    M(TYPE_TINYINT)                \
    M(TYPE_SMALLINT)               \
    M(TYPE_INT)                    \
    M(TYPE_BIGINT)                 \
    M(TYPE_FLOAT)                  \
    M(TYPE_DOUBLE)

namespace starrocks::vectorized {

template <PrimitiveType TYPE>
jvalue cast_to_jvalue(RunTimeCppType<TYPE> data_value, JVMFunctionHelper& helper);

#define DEFINE_CAST_TO_JVALUE(TYPE, APPLY_FUNC)                                                \
    template <>                                                                                \
    jvalue cast_to_jvalue<TYPE>(RunTimeCppType<TYPE> data_value, JVMFunctionHelper & helper) { \
        return {.l = APPLY_FUNC};                                                              \
    }

DEFINE_CAST_TO_JVALUE(TYPE_BOOLEAN, helper.newBoolean(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_TINYINT, helper.newByte(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_SMALLINT, helper.newShort(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_INT, helper.newInteger(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_BIGINT, helper.newLong(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_FLOAT, helper.newFloat(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_DOUBLE, helper.newDouble(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_VARCHAR, helper.newString(data_value.get_data(), data_value.get_size()));

void release_jvalue(MethodTypeDescriptor method_type_desc, jvalue val) {
    if (method_type_desc.is_box && val.l) {
        auto& helper = JVMFunctionHelper::getInstance();
        helper.getEnv()->DeleteLocalRef(val.l);
    }
}

// Used For UDAF
template <bool handle_null>
jvalue cast_to_jvalue(MethodTypeDescriptor method_type_desc, const Column* col, int row_num) {
    DCHECK(handle_null || !col->is_nullable());
    DCHECK(!col->is_constant());

    auto& helper = JVMFunctionHelper::getInstance();
    jvalue v;

    if constexpr (handle_null) {
        if (col->is_nullable()) {
            if (down_cast<const NullableColumn*>(col)->is_null(row_num)) {
                return {.l = nullptr};
            }
            col = down_cast<const NullableColumn*>(col)->data_column().get();
        }
    }

    DCHECK(!col->is_nullable());

    if (!method_type_desc.is_box) {
        switch (method_type_desc.type) {
#define M(NAME)                                                         \
    case NAME: {                                                        \
        auto spec_col = down_cast<const RunTimeColumnType<NAME>*>(col); \
        const auto& container = spec_col->get_data();                   \
        return cast_to_jvalue<NAME>(container[row_num], helper);        \
    }

            APPLY_FOR_NUMBERIC_TYPE(M)
#undef M
        default:
            DCHECK(false) << "udf unsupport type" << method_type_desc.type;
            v.l = nullptr;
            break;
        }
    } else {
        switch (method_type_desc.type) {
#define CREATE_BOX_TYPE(NAME, TYPE)                                     \
    case NAME: {                                                        \
        auto spec_col = down_cast<const RunTimeColumnType<NAME>*>(col); \
        const auto& container = spec_col->get_data();                   \
        return {.l = helper.new##TYPE(container[row_num])};             \
    }

            CREATE_BOX_TYPE(TYPE_BOOLEAN, Boolean)
            CREATE_BOX_TYPE(TYPE_TINYINT, Byte)
            CREATE_BOX_TYPE(TYPE_SMALLINT, Short)
            CREATE_BOX_TYPE(TYPE_INT, Integer)
            CREATE_BOX_TYPE(TYPE_BIGINT, Long)
            CREATE_BOX_TYPE(TYPE_FLOAT, Float)
            CREATE_BOX_TYPE(TYPE_DOUBLE, Double)

        case TYPE_VARCHAR: {
            auto spec_col = down_cast<const BinaryColumn*>(col);
            Slice slice = spec_col->get_slice(row_num);
            v.l = helper.newString(slice.get_data(), slice.get_size());
            break;
        }
        default:
            DCHECK(false) << "udf unsupport type" << method_type_desc.type;
            v.l = nullptr;
            break;
        }
    }
    return v;
}

template jvalue cast_to_jvalue<true>(MethodTypeDescriptor method_type_desc, const Column* col, int row_num);

template jvalue cast_to_jvalue<false>(MethodTypeDescriptor method_type_desc, const Column* col, int row_num);

void assign_jvalue(MethodTypeDescriptor method_type_desc, Column* col, int row_num, jvalue val) {
    DCHECK(method_type_desc.is_box);
    auto& helper = JVMFunctionHelper::getInstance();
    Column* data_col = col;
    if (col->is_nullable() && method_type_desc.type != PrimitiveType::TYPE_VARCHAR &&
        method_type_desc.type != PrimitiveType::TYPE_CHAR) {
        auto* nullable_column = down_cast<NullableColumn*>(col);
        if (val.l == nullptr) {
            nullable_column->set_null(row_num);
            return;
        }
        data_col = nullable_column->mutable_data_column();
    }
    switch (method_type_desc.type) {
#define ASSIGN_BOX_TYPE(NAME, TYPE)                                                \
    case NAME: {                                                                   \
        auto data = helper.val##TYPE(val.l);                                       \
        down_cast<RunTimeColumnType<NAME>*>(data_col)->get_data()[row_num] = data; \
        break;                                                                     \
    }

        ASSIGN_BOX_TYPE(TYPE_TINYINT, int8_t)
        ASSIGN_BOX_TYPE(TYPE_SMALLINT, int16_t)
        ASSIGN_BOX_TYPE(TYPE_INT, int32_t)
        ASSIGN_BOX_TYPE(TYPE_BIGINT, int64_t)
        ASSIGN_BOX_TYPE(TYPE_FLOAT, float)
        ASSIGN_BOX_TYPE(TYPE_DOUBLE, double)
    case TYPE_VARCHAR: {
        if (val.l == nullptr) {
            col->append_nulls(1);
        } else {
            auto slice = helper.sliceVal((jstring)val.l);
            col->append_datum(Datum(slice));
        }
        break;
    }

    default:
        DCHECK(false);
        break;
    }
}

void append_jvalue(MethodTypeDescriptor method_type_desc, Column* col, jvalue val) {
    auto& helper = JVMFunctionHelper::getInstance();
    if (!method_type_desc.is_box) {
        switch (method_type_desc.type) {
#define M(NAME)                                                                            \
    case NAME: {                                                                           \
        col->append_numbers(static_cast<const void*>(&val), sizeof(RunTimeCppType<NAME>)); \
    }
            APPLY_FOR_NUMBERIC_TYPE(M)
#undef M
        default:
            DCHECK(false) << "unsupport UDF TYPE" << method_type_desc.type;
            break;
        }
    } else {
        switch (method_type_desc.type) {
#define APPEND_BOX_TYPE(NAME, TYPE)          \
    case NAME: {                             \
        auto data = helper.val##TYPE(val.l); \
        col->append_datum(Datum(data));      \
        break;                               \
    }

            APPEND_BOX_TYPE(TYPE_BOOLEAN, uint8_t)
            APPEND_BOX_TYPE(TYPE_TINYINT, int8_t)
            APPEND_BOX_TYPE(TYPE_SMALLINT, int16_t)
            APPEND_BOX_TYPE(TYPE_INT, int32_t)
            APPEND_BOX_TYPE(TYPE_BIGINT, int64_t)
            APPEND_BOX_TYPE(TYPE_FLOAT, float)
            APPEND_BOX_TYPE(TYPE_DOUBLE, double)

        case TYPE_VARCHAR: {
            auto slice = helper.sliceVal((jstring)val.l);
            col->append_datum(Datum(slice));
            break;
        }
        default:
            DCHECK(false) << "unsupport UDF TYPE" << method_type_desc.type;
            break;
        }
    }
}

const int DEFAULT_UDAF_BUFFER_SIZE = 1024;

const AggregateFunction* getJavaUDAFFunction(bool input_nullable) {
    static JavaUDAFAggregateFunction<false> no_nullable_udaf_func;
    return &no_nullable_udaf_func;
}

Status init_udaf_context(int64_t id, const std::string& url, const std::string& checksum, const std::string& symbol,
                         starrocks_udf::FunctionContext* context) {
    std::string libpath;
    std::string state = symbol + "$State";
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_libpath(id, url, checksum, &libpath));
    auto* udaf_ctx = context->impl()->udaf_ctxs();
    udaf_ctx->udf_classloader = std::make_unique<ClassLoader>(std::move(libpath));
    RETURN_IF_ERROR(udaf_ctx->udf_classloader->init());
    udaf_ctx->analyzer = std::make_unique<ClassAnalyzer>();

    udaf_ctx->udaf_class = udaf_ctx->udf_classloader->getClass(symbol);
    if (udaf_ctx->udaf_class.clazz() == nullptr) {
        return Status::InternalError(fmt::format("couldn't found clazz:{}", symbol));
    }

    udaf_ctx->udaf_state_class = udaf_ctx->udf_classloader->getClass(state);
    if (udaf_ctx->udaf_state_class.clazz() == nullptr) {
        return Status::InternalError(fmt::format("couldn't found clazz:{}", state));
    }

    RETURN_IF_ERROR(udaf_ctx->udaf_class.newInstance(&udaf_ctx->handle));

    udaf_ctx->buffer_data.resize(DEFAULT_UDAF_BUFFER_SIZE);
    udaf_ctx->buffer = std::make_unique<DirectByteBuffer>(udaf_ctx->buffer_data.data(), udaf_ctx->buffer_data.size());

    auto* analyzer = udaf_ctx->analyzer.get();

    auto add_method = [&](const std::string& name, jclass clazz, std::unique_ptr<JavaMethodDescriptor>* res) {
        std::string method_name = name;
        std::string sign;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(analyzer->get_signature(clazz, method_name, &sign));
        RETURN_IF_ERROR(analyzer->get_udaf_method_desc(sign, &mtdesc));
        *res = std::make_unique<JavaMethodDescriptor>();
        (*res)->signature = std::move(sign);
        (*res)->name = std::move(method_name);
        (*res)->method_desc = std::move(mtdesc);
        ASSIGN_OR_RETURN((*res)->method, analyzer->get_method_object(clazz, name));
        return Status::OK();
    };

    RETURN_IF_ERROR(add_method("create", udaf_ctx->udaf_class.clazz(), &udaf_ctx->create));
    RETURN_IF_ERROR(add_method("destroy", udaf_ctx->udaf_class.clazz(), &udaf_ctx->destory));
    RETURN_IF_ERROR(add_method("update", udaf_ctx->udaf_class.clazz(), &udaf_ctx->update));
    RETURN_IF_ERROR(add_method("merge", udaf_ctx->udaf_class.clazz(), &udaf_ctx->merge));
    RETURN_IF_ERROR(add_method("finalize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->finalize));
    RETURN_IF_ERROR(add_method("serialize", udaf_ctx->udaf_class.clazz(), &udaf_ctx->serialize));
    RETURN_IF_ERROR(add_method("serializeLength", udaf_ctx->udaf_state_class.clazz(), &udaf_ctx->serialize_size));

    udaf_ctx->_func = std::make_unique<UDAFFunction>(udaf_ctx->handle, udaf_ctx);

    return Status::OK();
}

} // namespace starrocks::vectorized