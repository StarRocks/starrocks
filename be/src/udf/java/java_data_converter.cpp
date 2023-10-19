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

#include "udf/java/java_data_converter.h"

#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "common/compiler_util.h"
#include "common/status.h"

#define APPLY_FOR_NUMBERIC_TYPE(M) \
    M(TYPE_BOOLEAN)                \
    M(TYPE_TINYINT)                \
    M(TYPE_SMALLINT)               \
    M(TYPE_INT)                    \
    M(TYPE_BIGINT)                 \
    M(TYPE_FLOAT)                  \
    M(TYPE_DOUBLE)

namespace starrocks {

template <LogicalType TYPE>
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

void release_jvalue(bool is_box, jvalue val) {
    if (is_box && val.l) {
        auto& helper = JVMFunctionHelper::getInstance();
        helper.getEnv()->DeleteLocalRef(val.l);
    }
}

// Used For UDAF
template <bool handle_null>
jvalue cast_to_jvalue(LogicalType type, bool is_boxed, const Column* col, int row_num) {
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

    if (!is_boxed) {
        switch (type) {
#define M(NAME)                                                         \
    case NAME: {                                                        \
        auto spec_col = down_cast<const RunTimeColumnType<NAME>*>(col); \
        const auto& container = spec_col->get_data();                   \
        return cast_to_jvalue<NAME>(container[row_num], helper);        \
    }

            APPLY_FOR_NUMBERIC_TYPE(M)
#undef M
        default:
            DCHECK(false) << "udf unsupport type" << type;
            v.l = nullptr;
            break;
        }
    } else {
        switch (type) {
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
            DCHECK(false) << "udf unsupport type" << type;
            v.l = nullptr;
            break;
        }
    }
    return v;
}

template jvalue cast_to_jvalue<true>(LogicalType type, bool is_boxed, const Column* col, int row_num);

template jvalue cast_to_jvalue<false>(LogicalType type, bool is_boxed, const Column* col, int row_num);

void assign_jvalue(MethodTypeDescriptor method_type_desc, Column* col, int row_num, jvalue val) {
    DCHECK(method_type_desc.is_box);
    auto& helper = JVMFunctionHelper::getInstance();
    Column* data_col = col;
    if (col->is_nullable() && method_type_desc.type != LogicalType::TYPE_VARCHAR &&
        method_type_desc.type != LogicalType::TYPE_CHAR) {
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
    if (col->is_nullable() && val.l == nullptr) {
        col->append_nulls(1);
        return;
    }
    if (!method_type_desc.is_box) {
        switch (method_type_desc.type) {
#define M(NAME)                                                                                                        \
    case NAME: {                                                                                                       \
        [[maybe_unused]] auto ret = col->append_numbers(static_cast<const void*>(&val), sizeof(RunTimeCppType<NAME>)); \
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

Status ConvertDirectBufferVistor::do_visit(const NullableColumn& column) {
    const auto& null_data = column.immutable_null_column_data();
    _buffers.emplace_back((void*)null_data.data(), null_data.size());
    return column.data_column()->accept(this);
}

Status ConvertDirectBufferVistor::do_visit(const BinaryColumn& column) {
    const auto& offsets = column.get_offset();
    _buffers.emplace_back((void*)offsets.data(), offsets.size() * 4);
    const auto& bytes = column.get_bytes();
    _buffers.emplace_back((void*)bytes.data(), bytes.size());
    return Status::OK();
}

#define RETURN_NULL_WITH_REPORT_ERROR(cond, ctx, msg) \
    if (UNLIKELY(cond)) {                             \
        ctx->set_error(msg);                          \
    }

jobject JavaDataTypeConverter::convert_to_states(FunctionContext* ctx, uint8_t** data, size_t offset, int num_rows) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();
    int inputs[num_rows];
    jintArray arr = env->NewIntArray(num_rows);
    RETURN_NULL_WITH_REPORT_ERROR(arr == nullptr, ctx, "OOM may happened in Java Heap");
    for (int i = 0; i < num_rows; ++i) {
        inputs[i] = reinterpret_cast<JavaUDAFState*>(data[i] + offset)->handle;
    }
    env->SetIntArrayRegion(arr, 0, num_rows, inputs);
    return arr;
}

jobject JavaDataTypeConverter::convert_to_states_with_filter(FunctionContext* ctx, uint8_t** data, size_t offset,
                                                             const uint8_t* filter, int num_rows) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();
    int inputs[num_rows];
    jintArray arr = env->NewIntArray(num_rows);
    RETURN_NULL_WITH_REPORT_ERROR(arr == nullptr, ctx, "OOM may happened in Java Heap");
    for (int i = 0; i < num_rows; ++i) {
        if (filter[i] == 0) {
            inputs[i] = reinterpret_cast<JavaUDAFState*>(data[i] + offset)->handle;
        } else {
            inputs[i] = -1;
        }
    }
    env->SetIntArrayRegion(arr, 0, num_rows, inputs);
    return arr;
}

Status JavaDataTypeConverter::convert_to_boxed_array(FunctionContext* ctx, std::vector<DirectByteBuffer>* buffers,
                                                     const Column** columns, int num_cols, int num_rows,
                                                     std::vector<jobject>* res) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    ConvertDirectBufferVistor vistor(*buffers);
    LogicalType types[num_cols];
    for (int i = 0; i < num_cols; ++i) {
        types[i] = ctx->get_arg_type(i)->type;
        jobject arg = nullptr;
        if (columns[i]->only_null()) {
            arg = helper.create_array(num_rows);
        } else if (columns[i]->is_constant()) {
            auto& data_column = down_cast<const ConstColumn*>(columns[i])->data_column();
            data_column->resize(1);
            jobject jval = cast_to_jvalue<false>(types[i], true, data_column.get(), 0).l;
            arg = helper.create_object_array(jval, num_rows);
            env->DeleteLocalRef(jval);
        } else {
            int buffers_offset = buffers->size();
            RETURN_IF_ERROR(columns[i]->accept(&vistor));
            int buffers_sz = buffers->size() - buffers_offset;
            arg = helper.create_boxed_array(types[i], num_rows, columns[i]->is_nullable(), &(*buffers)[buffers_offset],
                                            buffers_sz);
        }

        if (arg == nullptr) {
            std::string err_msg = "OOM may happened in Java Heap";
            ctx->set_error(err_msg.c_str());
            return Status::InternalError(err_msg);
        }

        res->emplace_back(arg);
    }
    return Status::OK();
}
} // namespace starrocks
