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

#include <memory>
#include <type_traits>
#include <utility>

#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "types/date_value.h"
#include "types/datum.h"
#include "types/decimalv3.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "types/type_descriptor.h"
#include "udf/java/java_udf.h"
#include "udf/java/type_traits.h"

#define APPLY_FOR_NUMBERIC_TYPE(M) \
    M(TYPE_BOOLEAN)                \
    M(TYPE_TINYINT)                \
    M(TYPE_SMALLINT)               \
    M(TYPE_INT)                    \
    M(TYPE_BIGINT)                 \
    M(TYPE_FLOAT)                  \
    M(TYPE_DOUBLE)

namespace starrocks {

class JavaArrayConverter : public ColumnVisitorAdapter<JavaArrayConverter> {
public:
    JavaArrayConverter(JVMFunctionHelper& helper) : ColumnVisitorAdapter(this), _helper(helper) {}

    Status do_visit(const NullableColumn& column) {
        _nulls_buffer = get_buffer_data(*column.null_column_raw_ptr());
        return column.data_column()->accept(this);
    }

    Status do_visit(const BinaryColumn& column);
    Status do_visit(const ArrayColumn& column);
    Status do_visit(const MapColumn& column);

    template <typename T>
    Status do_visit(const FixedLengthColumn<T>& column) {
        if constexpr (JNIPrimTypeId<T>::supported) {
            size_t num_rows = column.size();
            std::unique_ptr<DirectByteBuffer> array_buffer = get_buffer_data(column);
            const auto& method_map = _helper.method_map();
            if (auto iter = method_map.find(JNIPrimTypeId<T>::id); iter != method_map.end()) {
                ASSIGN_OR_RETURN(_result, _helper.invoke_static_method(iter->second, num_rows, handle(_nulls_buffer),
                                                                       handle(array_buffer)));
                return Status::OK();
            }
        }

        return Status::NotSupported("unsupported UDF type");
    }

    // DECIMAL element columns (e.g. ARRAY<DECIMAL>) cannot reuse the primitive-int helpers
    // above: those produce Integer[] / Long[] arrays, not BigDecimal[]. Route them through
    // the DECIMAL-aware Java helper, taking precision/scale from the column itself.
    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        LogicalType logical_type = TYPE_UNKNOWN;
        if constexpr (std::is_same_v<T, int32_t>) {
            logical_type = TYPE_DECIMAL32;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            logical_type = TYPE_DECIMAL64;
        } else if constexpr (std::is_same_v<T, int128_t>) {
            logical_type = TYPE_DECIMAL128;
        } else if constexpr (std::is_same_v<T, int256_t>) {
            logical_type = TYPE_DECIMAL256;
        } else {
            return Status::NotSupported("unsupported decimal column width");
        }
        const auto container = column.immutable_data();
        auto data_buf = std::make_unique<DirectByteBuffer>((void*)container.data(), container.size() * sizeof(T));
        ASSIGN_OR_RETURN(_result, _helper.create_boxed_decimal_array(logical_type, column.scale(), column.size(),
                                                                     handle(_nulls_buffer), handle(data_buf)));
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const T& column) {
        return Status::NotSupported("UDF Not Support Type");
    }

    jobject result() { return _result; }

private:
    template <class ColumnType>
    std::unique_ptr<DirectByteBuffer> get_buffer_data(const ColumnType& column) {
        const auto container = column.immutable_data();
        return byte_buffer(container);
    }

    template <class T>
    std::unique_ptr<DirectByteBuffer> byte_buffer(const ImmBuffer<T>& buffer) {
        return std::make_unique<DirectByteBuffer>((void*)buffer.data(), buffer.size() * sizeof(T));
    }
    template <class T>
    std::unique_ptr<DirectByteBuffer> byte_buffer(const Buffer<T>& buffer) {
        return std::make_unique<DirectByteBuffer>((void*)buffer.data(), buffer.size() * sizeof(T));
    }
    template <class T>
    std::unique_ptr<DirectByteBuffer> byte_buffer(const starrocks::raw::RawVectorPad16<T, ColumnAllocator<T>>& buffer) {
        return std::make_unique<DirectByteBuffer>((void*)buffer.data(), buffer.size() * sizeof(T));
    }

    jobject handle(const std::unique_ptr<DirectByteBuffer>& byte_buffer) {
        if (byte_buffer == nullptr) {
            return nullptr;
        }
        return byte_buffer->handle();
    }

    jobject _result{};
    std::unique_ptr<DirectByteBuffer> _nulls_buffer;
    JVMFunctionHelper& _helper;
};

Status JavaArrayConverter::do_visit(const BinaryColumn& column) {
    size_t num_rows = column.size();
    auto bytes = byte_buffer(column.get_immutable_bytes());
    auto offsets = byte_buffer(column.get_offset());
    const auto& method_map = _helper.method_map();
    if (auto iter = method_map.find(JNIPrimTypeId<Slice>::id); iter != method_map.end()) {
        ASSIGN_OR_RETURN(_result, _helper.invoke_static_method(iter->second, num_rows, handle(_nulls_buffer),
                                                               handle(offsets), handle(bytes)));
        return Status::OK();
    }
    return Status::NotSupported("unsupported UDF type");
}

Status JavaArrayConverter::do_visit(const ArrayColumn& column) {
    size_t num_rows = column.size();
    auto offsets = byte_buffer(column.offsets().immutable_data());
    JavaArrayConverter converter(_helper);
    RETURN_IF_ERROR(column.elements_column()->accept(&converter));
    auto elements = converter.result();
    LOCAL_REF_GUARD(elements);
    const auto& method_map = _helper.method_map();
    if (auto iter = method_map.find(TYPE_ARRAY_METHOD_ID); iter != method_map.end()) {
        ASSIGN_OR_RETURN(_result, _helper.invoke_static_method(iter->second, num_rows, handle(_nulls_buffer),
                                                               handle(offsets), elements));
        return Status::OK();
    }
    return Status::NotSupported("unsupported UDF type");
}

Status JavaArrayConverter::do_visit(const MapColumn& column) {
    size_t num_rows = column.size();
    auto offsets = byte_buffer(column.offsets().immutable_data());
    JavaArrayConverter converter(_helper);
    RETURN_IF_ERROR(column.keys().accept(&converter));
    auto keys = converter.result();
    LOCAL_REF_GUARD(keys);
    RETURN_IF_ERROR(column.values().accept(&converter));
    auto values = converter.result();
    LOCAL_REF_GUARD(values);
    const auto& method_map = _helper.method_map();
    if (auto iter = method_map.find(TYPE_MAP_METHOD_ID); iter != method_map.end()) {
        ASSIGN_OR_RETURN(_result, _helper.invoke_static_method(iter->second, num_rows, handle(_nulls_buffer),
                                                               handle(offsets), keys, values));
        return Status::OK();
    }
    return Status::NotSupported("unsupported UDF type");
}

template <LogicalType TYPE>
jvalue cast_to_jvalue(RunTimeCppType<TYPE> data_value, JVMFunctionHelper& helper);

#define DEFINE_CAST_TO_JVALUE(TYPE, APPLY_FUNC)                                                \
    template <>                                                                                \
    jvalue cast_to_jvalue<TYPE>(RunTimeCppType<TYPE> data_value, JVMFunctionHelper & helper) { \
        return {.l = APPLY_FUNC};                                                              \
    }

// Build a java.math.BigDecimal jobject for row `row_num` of a DECIMAL column. Unified helper
// used by both the single-value path (cast_to_jvalue) and the batch const-column path.
static jobject decimal_cell_to_bigdecimal(const TypeDescriptor& td, const Column* col, int row_num,
                                          JVMFunctionHelper& helper) {
    switch (td.type) {
    case TYPE_DECIMAL32: {
        auto* spec = down_cast<const RunTimeColumnType<TYPE_DECIMAL32>*>(col);
        return helper.newBigDecimal(static_cast<int64_t>(spec->immutable_data()[row_num]), td.scale);
    }
    case TYPE_DECIMAL64: {
        auto* spec = down_cast<const RunTimeColumnType<TYPE_DECIMAL64>*>(col);
        return helper.newBigDecimal(spec->immutable_data()[row_num], td.scale);
    }
    case TYPE_DECIMAL128: {
        auto* spec = down_cast<const RunTimeColumnType<TYPE_DECIMAL128>*>(col);
        return helper.newBigDecimal(
                DecimalV3Cast::to_string<int128_t>(spec->immutable_data()[row_num], td.precision, td.scale));
    }
    case TYPE_DECIMAL256: {
        auto* spec = down_cast<const RunTimeColumnType<TYPE_DECIMAL256>*>(col);
        return helper.newBigDecimal(
                DecimalV3Cast::to_string<int256_t>(spec->immutable_data()[row_num], td.precision, td.scale));
    }
    default:
        DCHECK(false) << "unsupported decimal type: " << td.type;
        return nullptr;
    }
}

// Parse a BigDecimal's string form into a native DECIMAL column using the column's
// declared precision/scale. On overflow: error if `error_if_overflow`, else append NULL.
static Status append_decimal_string_to_column(const TypeDescriptor& td, const std::string& str, Column* col,
                                              bool error_if_overflow) {
    Datum datum;
    bool err = false;
#define APPEND_DECIMAL_CASE(LOGICAL_TYPE, CPP_TYPE)                                                     \
    case LOGICAL_TYPE: {                                                                                \
        CPP_TYPE v{};                                                                                   \
        err = DecimalV3Cast::from_string<CPP_TYPE>(&v, td.precision, td.scale, str.data(), str.size()); \
        datum = Datum(v);                                                                               \
        break;                                                                                          \
    }
    switch (td.type) {
        APPEND_DECIMAL_CASE(TYPE_DECIMAL32, int32_t)
        APPEND_DECIMAL_CASE(TYPE_DECIMAL64, int64_t)
        APPEND_DECIMAL_CASE(TYPE_DECIMAL128, int128_t)
        APPEND_DECIMAL_CASE(TYPE_DECIMAL256, int256_t)
    default:
        return Status::NotSupported(fmt::format("unsupported decimal type: {}", td.type));
    }
#undef APPEND_DECIMAL_CASE
    if (err) {
        if (error_if_overflow) {
            return Status::InvalidArgument(
                    fmt::format("Cannot parse '{}' into DECIMAL({},{})", str, td.precision, td.scale));
        }
        col->append_nulls(1);
        return Status::OK();
    }
    col->append_datum(datum);
    return Status::OK();
}

DEFINE_CAST_TO_JVALUE(TYPE_BOOLEAN, helper.newBoolean(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_TINYINT, helper.newByte(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_SMALLINT, helper.newShort(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_INT, helper.newInteger(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_BIGINT, helper.newLong(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_FLOAT, helper.newFloat(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_DOUBLE, helper.newDouble(data_value));
DEFINE_CAST_TO_JVALUE(TYPE_VARCHAR, helper.newString(data_value.get_data(), data_value.get_size()));
DEFINE_CAST_TO_JVALUE(TYPE_DATE, helper.newLocalDate(data_value._julian));
DEFINE_CAST_TO_JVALUE(TYPE_DATETIME, helper.newLocalDateTime(data_value.timestamp()));

void release_jvalue(bool is_box, jvalue val) {
    if (is_box && val.l) {
        auto& helper = JVMFunctionHelper::getInstance();
        helper.getEnv()->DeleteLocalRef(val.l);
    }
}

// Used in UDAF/convert const columns
StatusOr<jvalue> cast_to_jvalue(const TypeDescriptor& type_desc, bool is_boxed, const Column* col, int row_num) {
    DCHECK(!col->is_constant());

    auto type = type_desc.type;
    auto& helper = JVMFunctionHelper::getInstance();
    jvalue v;

    if (col->is_nullable()) {
        if (down_cast<const NullableColumn*>(col)->is_null(row_num)) {
            return jvalue{.l = nullptr};
        }
        col = down_cast<const NullableColumn*>(col)->data_column().get();
    }

    DCHECK(!col->is_nullable());

    if (!is_boxed) {
        switch (type) {
#define M(NAME)                                                         \
    case NAME: {                                                        \
        auto spec_col = down_cast<const RunTimeColumnType<NAME>*>(col); \
        const auto container = spec_col->immutable_data();              \
        return cast_to_jvalue<NAME>(container[row_num], helper);        \
    }

            APPLY_FOR_NUMBERIC_TYPE(M)
#undef M
        default:
            DCHECK(false) << "udf unsupport type" << type;
            return jvalue{.l = nullptr};
        }
    }

    switch (type) {
#define CREATE_BOX_TYPE(NAME, TYPE)                                     \
    case NAME: {                                                        \
        auto spec_col = down_cast<const RunTimeColumnType<NAME>*>(col); \
        const auto container = spec_col->immutable_data();              \
        return jvalue{.l = helper.new##TYPE(container[row_num])};       \
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
    case TYPE_DATE: {
        auto spec_col = down_cast<const RunTimeColumnType<TYPE_DATE>*>(col);
        v.l = helper.newLocalDate(spec_col->immutable_data()[row_num]._julian);
        break;
    }
    case TYPE_DATETIME: {
        auto spec_col = down_cast<const RunTimeColumnType<TYPE_DATETIME>*>(col);
        v.l = helper.newLocalDateTime(spec_col->immutable_data()[row_num].timestamp());
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_DECIMAL256: {
        v.l = decimal_cell_to_bigdecimal(type_desc, col, row_num, helper);
        break;
    }
    case TYPE_ARRAY: {
        auto spec_col = down_cast<const ArrayColumn*>(col);
        auto [offset, size] = spec_col->get_element_offset_size(row_num);
        const ListMeta& meta = helper.list_meta();
        ASSIGN_OR_RETURN(auto object, meta.array_list_class->newLocalInstance());
        LOCAL_REF_GUARD(object);
        JavaListStub list_stub(object);
        for (size_t i = offset; i < offset + size; ++i) {
            ASSIGN_OR_RETURN(auto e, cast_to_jvalue(type_desc.children[0], true, spec_col->elements_column().get(), i));
            auto local_obj = e.l;
            LOCAL_REF_GUARD(local_obj);
            RETURN_IF_ERROR(list_stub.add(local_obj));
        }
        auto res = jvalue{.l = object};
        object = nullptr;
        return res;
    }
    case TYPE_MAP: {
        auto spec_col = down_cast<const MapColumn*>(col);
        auto [offset, size] = spec_col->get_map_offset_size(row_num);
        const ListMeta& meta = helper.list_meta();
        ASSIGN_OR_RETURN(auto key_lists, meta.array_list_class->newLocalInstance());
        LOCAL_REF_GUARD(key_lists);
        JavaListStub key_list_stub(key_lists);

        ASSIGN_OR_RETURN(auto val_lists, meta.array_list_class->newLocalInstance());
        LOCAL_REF_GUARD(val_lists);
        JavaListStub val_list_stub(val_lists);

        for (size_t i = offset; i < offset + size; ++i) {
            ASSIGN_OR_RETURN(auto key, cast_to_jvalue(type_desc.children[0], true, spec_col->keys_column().get(), i));
            auto key_obj = key.l;
            LOCAL_REF_GUARD(key_obj);
            RETURN_IF_ERROR(key_list_stub.add(key_obj));

            ASSIGN_OR_RETURN(auto value,
                             cast_to_jvalue(type_desc.children[1], true, spec_col->values_column().get(), i));
            auto value_obj = value.l;
            LOCAL_REF_GUARD(value_obj);
            RETURN_IF_ERROR(val_list_stub.add(value_obj));
        }
        const MapMeta& immutable_map_meta = helper.map_meta();
        ASSIGN_OR_RETURN(auto m, immutable_map_meta.newLocalInstance(key_lists, val_lists));
        auto res = jvalue{.l = m};
        return res;
    }
    default:
        DCHECK(false) << "unsupported UDF type:" << type;
        v.l = nullptr;
        break;
    }
    return v;
}

// Translate a JNI-side ArithmeticException raised by UDFHelper.unscaledLong /
// UDFHelper.unscaledLEBytes into either a non-OK Status (REPORT_ERROR) or a NULL row
// (OUTPUT_NULL). Always clears the pending exception so the JVM is left in a clean state.
static Status handle_decimal_overflow(JNIEnv* env, const TypeDescriptor& td, Column* col, int row_num,
                                      bool error_if_overflow) {
    if (!env->ExceptionCheck()) {
        return Status::OK();
    }
    if (error_if_overflow) {
        auto& helper = JVMFunctionHelper::getInstance();
        std::string msg;
        if (jthrowable jthr = env->ExceptionOccurred(); jthr != nullptr) {
            msg = helper.dumpExceptionString(jthr);
            env->DeleteLocalRef(jthr);
        }
        env->ExceptionClear();
        return Status::InvalidArgument(fmt::format("DECIMAL({},{}) overflow: {}", td.precision, td.scale, msg));
    }
    env->ExceptionClear();
    if (col->is_nullable()) {
        down_cast<NullableColumn*>(col)->set_null(row_num);
    }
    return Status::OK();
}

Status assign_jvalue(const TypeDescriptor& type_desc, bool is_box, Column* col, int row_num, jvalue val,
                     bool error_if_overflow) {
    DCHECK(is_box);
    auto& helper = JVMFunctionHelper::getInstance();
    Column* data_col = col;
    if (col->is_nullable() && type_desc.type != LogicalType::TYPE_VARCHAR && type_desc.type != LogicalType::TYPE_CHAR) {
        auto* nullable_column = down_cast<NullableColumn*>(col);
        if (val.l == nullptr) {
            nullable_column->set_null(row_num);
            return Status::OK();
        }
        data_col = nullable_column->data_column_raw_ptr();
    }
    switch (type_desc.type) {
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
    case TYPE_DATE: {
        DateValue dv;
        dv._julian = helper.valLocalDate(val.l);
        down_cast<RunTimeColumnType<TYPE_DATE>*>(data_col)->get_data()[row_num] = dv;
        break;
    }
    case TYPE_DATETIME: {
        TimestampValue tv;
        tv.set_timestamp(helper.valLocalDateTime(val.l));
        down_cast<RunTimeColumnType<TYPE_DATETIME>*>(data_col)->get_data()[row_num] = tv;
        break;
    }
    case TYPE_VARCHAR: {
        if (val.l == nullptr) {
            col->append_nulls(1);
        } else {
            std::string buffer;
            auto slice = helper.sliceVal((jstring)val.l, &buffer);
            col->append_datum(Datum(slice));
        }
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64: {
        // DECIMAL32/64: ask Java for the unscaled value as a long. If the helper raised
        // an ArithmeticException (precision overflow or value > long range), translate it
        // — `unscaled` and the data column slot must not be touched in that branch.
        auto* env = helper.getEnv();
        jlong unscaled = helper.unscaled_long(val.l, type_desc.precision, type_desc.scale);
        if (env->ExceptionCheck()) {
            return handle_decimal_overflow(env, type_desc, col, row_num, error_if_overflow);
        }
        if (type_desc.type == TYPE_DECIMAL32) {
            down_cast<RunTimeColumnType<TYPE_DECIMAL32>*>(data_col)->get_data()[row_num] =
                    static_cast<int32_t>(unscaled);
        } else {
            down_cast<RunTimeColumnType<TYPE_DECIMAL64>*>(data_col)->get_data()[row_num] =
                    static_cast<int64_t>(unscaled);
        }
        break;
    }
    case TYPE_DECIMAL128:
    case TYPE_DECIMAL256: {
        // DECIMAL128/256: ask Java for sign-extended little-endian bytes; copy directly into
        // the int128/int256 cell at row_num. Same overflow short-circuit as the narrow path
        // — `bytes` is null when the helper threw, so we must not fall through to
        // GetByteArrayRegion.
        auto* env = helper.getEnv();
        const int byte_width = (type_desc.type == TYPE_DECIMAL128) ? 16 : 32;
        jbyteArray bytes = helper.unscaled_le_bytes(val.l, type_desc.precision, type_desc.scale, byte_width);
        if (env->ExceptionCheck()) {
            return handle_decimal_overflow(env, type_desc, col, row_num, error_if_overflow);
        }
        DCHECK(bytes != nullptr);
        LOCAL_REF_GUARD(bytes);
        if (type_desc.type == TYPE_DECIMAL128) {
            int128_t v;
            env->GetByteArrayRegion(bytes, 0, 16, reinterpret_cast<jbyte*>(&v));
            down_cast<RunTimeColumnType<TYPE_DECIMAL128>*>(data_col)->get_data()[row_num] = v;
        } else {
            int256_t v;
            env->GetByteArrayRegion(bytes, 0, 32, reinterpret_cast<jbyte*>(&v));
            down_cast<RunTimeColumnType<TYPE_DECIMAL256>*>(data_col)->get_data()[row_num] = v;
        }
        break;
    }
    default:
        DCHECK(false);
        break;
    }
    return Status::OK();
}

Status append_jvalue(const TypeDescriptor& type_desc, bool is_box, Column* col, jvalue val, bool error_if_overflow) {
    auto& helper = JVMFunctionHelper::getInstance();
    if (col->is_nullable() && val.l == nullptr) {
        col->append_nulls(1);
        return Status::OK();
    }
    if (!is_box) {
        switch (type_desc.type) {
#define M(NAME)                                                                                                        \
    case NAME: {                                                                                                       \
        [[maybe_unused]] auto ret = col->append_numbers(static_cast<const void*>(&val), sizeof(RunTimeCppType<NAME>)); \
    }
            APPLY_FOR_NUMBERIC_TYPE(M)
#undef M
        default:
            DCHECK(false) << "unsupport UDF TYPE" << type_desc.type;
            break;
        }
    } else {
        switch (type_desc.type) {
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

        case TYPE_DATE: {
            DateValue dv;
            dv._julian = helper.valLocalDate(val.l);
            col->append_datum(Datum(dv));
            break;
        }
        case TYPE_DATETIME: {
            TimestampValue tv;
            tv.set_timestamp(helper.valLocalDateTime(val.l));
            col->append_datum(Datum(tv));
            break;
        }
        case TYPE_VARCHAR: {
            std::string buffer;
            auto slice = helper.sliceVal((jstring)val.l, &buffer);
            col->append_datum(Datum(slice));
            break;
        }
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128:
        case TYPE_DECIMAL256: {
            RETURN_IF_ERROR(
                    append_decimal_string_to_column(type_desc, helper.to_string(val.l), col, error_if_overflow));
            break;
        }
        case TYPE_ARRAY: {
            JavaListStub list_stub(val.l);
            ASSIGN_OR_RETURN(auto len, list_stub.size());
            if (col->is_nullable()) {
                down_cast<NullableColumn*>(col)->null_column_data().emplace_back(0);
            }
            auto* data_column = ColumnHelper::get_data_column(col);
            auto* array_column = down_cast<ArrayColumn*>(data_column);
            for (size_t i = 0; i < len; ++i) {
                ASSIGN_OR_RETURN(auto element, list_stub.get(i));
                RETURN_IF_ERROR(append_jvalue(type_desc.children[0], true, array_column->elements_column_raw_ptr(),
                                              {.l = element}));
            }
            auto* offsets_col = array_column->offsets_column_raw_ptr();
            size_t last_offset = offsets_col->get_data().back();
            offsets_col->get_data().push_back(last_offset + len);
            break;
        }
        case TYPE_MAP: {
            if (col->is_nullable()) {
                down_cast<NullableColumn*>(col)->null_column_data().emplace_back(0);
            }
            auto* data_column = ColumnHelper::get_data_column(col);
            auto* map_column = down_cast<MapColumn*>(data_column);

            // extract map object to list
            ASSIGN_OR_RETURN(jobject key_list, helper.extract_key_list(val.l));
            ASSIGN_OR_RETURN(jobject val_list, helper.extract_val_list(val.l));

            JavaListStub key_list_stub(key_list);
            JavaListStub val_list_stub(val_list);

            ASSIGN_OR_RETURN(auto len, key_list_stub.size());

            for (size_t i = 0; i < len; ++i) {
                ASSIGN_OR_RETURN(auto key_element, key_list_stub.get(i));
                RETURN_IF_ERROR(append_jvalue(type_desc.children[0], true, map_column->keys_column_raw_ptr(),
                                              {.l = key_element}));

                ASSIGN_OR_RETURN(auto val_element, val_list_stub.get(i));
                RETURN_IF_ERROR(append_jvalue(type_desc.children[1], true, map_column->values_column_raw_ptr(),
                                              {.l = val_element}));
            }

            auto* offsets_col = map_column->offsets_column_raw_ptr();
            size_t last_offset = offsets_col->get_data().back();
            offsets_col->get_data().push_back(last_offset + len);
            break;
        }
        default:
            DCHECK(false) << "unsupport UDF TYPE" << type_desc.type;
            return Status::NotSupported(fmt::format("unsupport UDF TYPE:{}", type_desc.type));
        }
    }
    return Status::OK();
}

Status check_type_matched(const TypeDescriptor& type_desc, jobject val) {
    if (val == nullptr) {
        return Status::OK();
    }
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    switch (type_desc.type) {
#define INSTANCE_OF_TYPE(NAME, TYPE)                                                            \
    case NAME: {                                                                                \
        if (!env->IsInstanceOf(val, helper.TYPE##_class())) {                                   \
            auto clazz = env->GetObjectClass(val);                                              \
            LOCAL_REF_GUARD(clazz);                                                             \
            return Status::InternalError(fmt::format("Type not matched, expect {}, but got {}", \
                                                     helper.to_string(helper.TYPE##_class()),   \
                                                     helper.to_string(clazz)));                 \
        }                                                                                       \
        break;                                                                                  \
    }
        INSTANCE_OF_TYPE(TYPE_BOOLEAN, uint8_t)
        INSTANCE_OF_TYPE(TYPE_TINYINT, int8_t)
        INSTANCE_OF_TYPE(TYPE_SMALLINT, int16_t)
        INSTANCE_OF_TYPE(TYPE_INT, int32_t)
        INSTANCE_OF_TYPE(TYPE_BIGINT, int64_t)
        INSTANCE_OF_TYPE(TYPE_FLOAT, float)
        INSTANCE_OF_TYPE(TYPE_DOUBLE, double)
    case TYPE_VARCHAR: {
        if (!env->IsInstanceOf(val, helper.string_clazz())) {
            auto clazz = env->GetObjectClass(val);
            LOCAL_REF_GUARD(clazz);
            return Status::InternalError(
                    fmt::format("Type not matched, expect string, but got {}", helper.to_string(clazz)));
        }
        break;
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_DECIMAL256: {
        if (!env->IsInstanceOf(val, helper.big_decimal_class())) {
            auto clazz = env->GetObjectClass(val);
            LOCAL_REF_GUARD(clazz);
            return Status::InternalError(
                    fmt::format("Type not matched, expect java.math.BigDecimal, but got {}", helper.to_string(clazz)));
        }
        break;
    }
    case TYPE_DATE: {
        if (!env->IsInstanceOf(val, helper.local_date_class())) {
            auto clazz = env->GetObjectClass(val);
            LOCAL_REF_GUARD(clazz);
            return Status::InternalError(
                    fmt::format("Type not matched, expect java.time.LocalDate, but got {}", helper.to_string(clazz)));
        }
        break;
    }
    case TYPE_DATETIME: {
        if (!env->IsInstanceOf(val, helper.local_datetime_class())) {
            auto clazz = env->GetObjectClass(val);
            LOCAL_REF_GUARD(clazz);
            return Status::InternalError(fmt::format("Type not matched, expect java.time.LocalDateTime, but got {}",
                                                     helper.to_string(clazz)));
        }
        break;
    }
    case TYPE_ARRAY: {
        if (!env->IsInstanceOf(val, helper.list_meta().list_class->clazz())) {
            auto clazz = env->GetObjectClass(val);
            LOCAL_REF_GUARD(clazz);
            return Status::InternalError(
                    fmt::format("Type not matched, expect List, but got {}", helper.to_string(clazz)));
        }
        break;
    }
    case TYPE_MAP: {
        if (!env->IsInstanceOf(val, helper.map_meta().map_class->clazz())) {
            auto clazz = env->GetObjectClass(val);
            LOCAL_REF_GUARD(clazz);
            return Status::InternalError(
                    fmt::format("Type not matched, expect Map, but got {}", helper.to_string(clazz)));
        }
        break;
    }
    default:
        DCHECK(false) << "unsupport UDF TYPE" << type_desc.type;
        break;
    }

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

// Wrap a DECIMAL column's raw storage as a DirectByteBuffer over `count` contiguous
// unscaled integers of the expected cpp width.
template <LogicalType TYPE>
static std::unique_ptr<DirectByteBuffer> wrap_decimal_data(const Column* data_column) {
    const auto* spec = down_cast<const RunTimeColumnType<TYPE>*>(data_column);
    const auto container = spec->immutable_data();
    using CppType = RunTimeCppType<TYPE>;
    return std::make_unique<DirectByteBuffer>((void*)container.data(), container.size() * sizeof(CppType));
}

// Build a BigDecimal[] for a DECIMAL* input column by handing the raw unscaled-integer buffer
// (and null buffer, if any) to the Java helper together with the column scale.
static StatusOr<jobject> build_decimal_boxed_array(const TypeDescriptor& type_desc, const Column* column,
                                                   int num_rows) {
    auto& helper = JVMFunctionHelper::getInstance();

    const Column* data_column = column;
    std::unique_ptr<DirectByteBuffer> null_buf;
    if (column->is_nullable()) {
        const auto* nullable_column = down_cast<const NullableColumn*>(column);
        const auto null_data = nullable_column->null_column_raw_ptr()->immutable_data();
        null_buf = std::make_unique<DirectByteBuffer>((void*)null_data.data(), null_data.size() * sizeof(uint8_t));
        data_column = nullable_column->data_column().get();
    }

    std::unique_ptr<DirectByteBuffer> data_buf;
    switch (type_desc.type) {
    case TYPE_DECIMAL32:
        data_buf = wrap_decimal_data<TYPE_DECIMAL32>(data_column);
        break;
    case TYPE_DECIMAL64:
        data_buf = wrap_decimal_data<TYPE_DECIMAL64>(data_column);
        break;
    case TYPE_DECIMAL128:
        data_buf = wrap_decimal_data<TYPE_DECIMAL128>(data_column);
        break;
    case TYPE_DECIMAL256:
        // int256_t layout is {uint128_t low; int128_t high;}, giving 32 little-endian bytes.
        static_assert(sizeof(RunTimeCppType<TYPE_DECIMAL256>) == 32, "int256_t must be 32 bytes for DECIMAL256 layout");
        data_buf = wrap_decimal_data<TYPE_DECIMAL256>(data_column);
        break;
    default:
        return Status::NotSupported(fmt::format("unsupported decimal type: {}", type_desc.type));
    }

    jobject null_handle = null_buf ? null_buf->handle() : nullptr;
    return helper.create_boxed_decimal_array(type_desc.type, type_desc.scale, num_rows, null_handle,
                                             data_buf->handle());
}

Status JavaDataTypeConverter::convert_to_boxed_array(FunctionContext* ctx, const Column** columns, int num_cols,
                                                     int num_rows, std::vector<jobject>* res) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    for (int i = 0; i < num_cols; ++i) {
        jobject arg = nullptr;
        const TypeDescriptor& arg_type = *ctx->get_arg_type(i);
        const bool is_decimal = is_decimalv3_field_type(arg_type.type);
        if (columns[i]->only_null() ||
            (columns[i]->is_nullable() && down_cast<const NullableColumn*>(columns[i])->null_count() == num_rows)) {
            arg = helper.create_array(num_rows);
        } else if (columns[i]->is_constant()) {
            auto* data_column = down_cast<const ConstColumn*>(columns[i])->data_column_raw_ptr();
            data_column->as_mutable_raw_ptr()->resize(1);
            ASSIGN_OR_RETURN(jvalue jval, cast_to_jvalue(arg_type, true, data_column, 0));
            arg = helper.create_object_array(jval.l, num_rows);
            env->DeleteLocalRef(jval.l);
        } else if (is_decimal) {
            ASSIGN_OR_RETURN(arg, build_decimal_boxed_array(arg_type, columns[i], num_rows));
        } else {
            JavaArrayConverter converter(helper);
            RETURN_IF_ERROR(columns[i]->accept(&converter));
            arg = converter.result();
        }

        res->emplace_back(arg);
    }
    return Status::OK();
}
} // namespace starrocks
