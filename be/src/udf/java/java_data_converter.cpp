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
#include "column/struct_column.h"
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

// Forward declarations for helpers that read fields off a UdfTypeDesc jobject.
// Defined later in this file, but we need them in cast_to_jvalue / append_jvalue /
// check_type_matched for STRUCT support.
static jobject get_type_desc_child(JVMFunctionHelper& helper, jobject type_desc, int index);
static jclass get_type_desc_record_class(JVMFunctionHelper& helper, jobject type_desc);

// Used in UDAF/UDTF per-row paths and convert const columns.
//
// For TYPE_STRUCT, `type_desc_obj` (a UdfTypeDesc jobject) supplies the formal Java
// record class so we can construct a record instance via UDFHelper.createBoxedStructArray
// (re-used for the single-row case by feeding it 1-element field arrays). For non-STRUCT
// subtrees `type_desc_obj` may be null and the existing scalar/array/map paths run
// unchanged.
StatusOr<jvalue> cast_to_jvalue(const TypeDescriptor& type_desc, bool is_boxed, const Column* col, int row_num,
                                jobject type_desc_obj) {
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
        jobject elem_desc = get_type_desc_child(helper, type_desc_obj, 0);
        DeferOp drop_elem_desc([&]() {
            if (elem_desc) helper.getEnv()->DeleteLocalRef(elem_desc);
        });
        for (size_t i = offset; i < offset + size; ++i) {
            ASSIGN_OR_RETURN(auto e, cast_to_jvalue(type_desc.children[0], true, spec_col->elements_column().get(), i,
                                                    elem_desc));
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

        jobject key_desc = get_type_desc_child(helper, type_desc_obj, 0);
        DeferOp drop_key_desc([&]() {
            if (key_desc) helper.getEnv()->DeleteLocalRef(key_desc);
        });
        jobject val_desc = get_type_desc_child(helper, type_desc_obj, 1);
        DeferOp drop_val_desc([&]() {
            if (val_desc) helper.getEnv()->DeleteLocalRef(val_desc);
        });
        for (size_t i = offset; i < offset + size; ++i) {
            ASSIGN_OR_RETURN(auto key,
                             cast_to_jvalue(type_desc.children[0], true, spec_col->keys_column().get(), i, key_desc));
            auto key_obj = key.l;
            LOCAL_REF_GUARD(key_obj);
            RETURN_IF_ERROR(key_list_stub.add(key_obj));

            ASSIGN_OR_RETURN(auto value,
                             cast_to_jvalue(type_desc.children[1], true, spec_col->values_column().get(), i, val_desc));
            auto value_obj = value.l;
            LOCAL_REF_GUARD(value_obj);
            RETURN_IF_ERROR(val_list_stub.add(value_obj));
        }
        const MapMeta& immutable_map_meta = helper.map_meta();
        ASSIGN_OR_RETURN(auto m, immutable_map_meta.newLocalInstance(key_lists, val_lists));
        auto res = jvalue{.l = m};
        return res;
    }
    case TYPE_STRUCT: {
        if (type_desc_obj == nullptr) {
            return Status::InternalError("STRUCT cast_to_jvalue requires a UdfTypeDesc with the formal record class");
        }
        jclass record_class = get_type_desc_record_class(helper, type_desc_obj);
        if (record_class == nullptr) {
            return Status::InternalError("STRUCT cast_to_jvalue: missing formal record class on UdfTypeDesc");
        }
        DeferOp drop_record_class([&]() { helper.getEnv()->DeleteLocalRef(record_class); });

        if (!col->is_struct()) {
            return Status::InternalError("expected StructColumn for STRUCT slot");
        }
        const auto* struct_col = down_cast<const StructColumn*>(col);
        int num_fields = static_cast<int>(struct_col->fields_size());
        if (static_cast<int>(type_desc.children.size()) != num_fields) {
            return Status::InternalError("STRUCT cast_to_jvalue: SQL field count != column field count");
        }

        // Build a single-row Object[1] for each subfield. We re-use the existing
        // batched helper UDFHelper.createBoxedStructArray with num_rows=1 instead of
        // bringing up a parallel single-row record-construction path: it takes
        // per-field Object[1] arrays and a null bitmap and returns Object[1] holding
        // the constructed record (or null when row 0 is null).
        JNIEnv* env = helper.getEnv();
        jclass object_clazz = env->FindClass("java/lang/Object");
        DeferOp drop_object_clazz([&]() { env->DeleteLocalRef(object_clazz); });
        jobjectArray field_arrays = env->NewObjectArray(num_fields, object_clazz, nullptr);
        if (field_arrays == nullptr) {
            return Status::InternalError("failed to allocate field_arrays for STRUCT cast_to_jvalue");
        }
        DeferOp drop_field_arrays([&]() { env->DeleteLocalRef(field_arrays); });

        for (int f = 0; f < num_fields; ++f) {
            const Column* field_col = struct_col->field_column_raw_ptr(f);
            jobject child_desc = get_type_desc_child(helper, type_desc_obj, f);
            DeferOp drop_child_desc([&]() {
                if (child_desc) env->DeleteLocalRef(child_desc);
            });
            ASSIGN_OR_RETURN(jvalue field_val,
                             cast_to_jvalue(type_desc.children[f], true, field_col, row_num, child_desc));
            // Wrap the single field value in a 1-element Object[].
            jobjectArray cell = env->NewObjectArray(1, object_clazz, field_val.l);
            release_jvalue(true, field_val);
            if (cell == nullptr) {
                return Status::InternalError("failed to allocate cell array for STRUCT field");
            }
            env->SetObjectArrayElement(field_arrays, f, cell);
            env->DeleteLocalRef(cell);
        }

        // STRUCT cell may itself be null; the parent NullableColumn was already peeled
        // by the prologue of cast_to_jvalue, so `col` here is the data column. No null
        // bitmap is needed at this point — we always have a non-null struct value.
        ASSIGN_OR_RETURN(jobject out_arr, helper.create_boxed_struct_array(record_class, /*num_rows=*/1,
                                                                           /*null_buff=*/nullptr, field_arrays));
        DeferOp drop_out_arr([&]() { env->DeleteLocalRef(out_arr); });
        // out_arr is Object[1] containing the constructed record. Lift index 0 out as
        // a fresh local ref since the array itself is dropped on scope exit.
        jobject record = env->GetObjectArrayElement((jobjectArray)out_arr, 0);
        v.l = record;
        break;
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

Status append_jvalue(const TypeDescriptor& type_desc, bool is_box, Column* col, jvalue val, bool error_if_overflow,
                     jobject type_desc_obj) {
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
            jobject elem_desc = get_type_desc_child(helper, type_desc_obj, 0);
            DeferOp drop_elem_desc([&]() {
                if (elem_desc) helper.getEnv()->DeleteLocalRef(elem_desc);
            });
            for (size_t i = 0; i < len; ++i) {
                ASSIGN_OR_RETURN(auto element, list_stub.get(i));
                RETURN_IF_ERROR(append_jvalue(type_desc.children[0], true, array_column->elements_column_raw_ptr(),
                                              {.l = element}, error_if_overflow, elem_desc));
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

            jobject key_desc = get_type_desc_child(helper, type_desc_obj, 0);
            DeferOp drop_key_desc([&]() {
                if (key_desc) helper.getEnv()->DeleteLocalRef(key_desc);
            });
            jobject val_desc = get_type_desc_child(helper, type_desc_obj, 1);
            DeferOp drop_val_desc([&]() {
                if (val_desc) helper.getEnv()->DeleteLocalRef(val_desc);
            });
            for (size_t i = 0; i < len; ++i) {
                ASSIGN_OR_RETURN(auto key_element, key_list_stub.get(i));
                RETURN_IF_ERROR(append_jvalue(type_desc.children[0], true, map_column->keys_column_raw_ptr(),
                                              {.l = key_element}, error_if_overflow, key_desc));

                ASSIGN_OR_RETURN(auto val_element, val_list_stub.get(i));
                RETURN_IF_ERROR(append_jvalue(type_desc.children[1], true, map_column->values_column_raw_ptr(),
                                              {.l = val_element}, error_if_overflow, val_desc));
            }

            auto* offsets_col = map_column->offsets_column_raw_ptr();
            size_t last_offset = offsets_col->get_data().back();
            offsets_col->get_data().push_back(last_offset + len);
            break;
        }
        case TYPE_STRUCT: {
            // Recurse into each subfield by reading the matching record component
            // accessor and forwarding the boxed value to the subfield column. This
            // mirrors the per-row drain logic that writeResult does in batch on
            // the Java side, but stays per-row to fit the existing UDTF/UDAF
            // single-row append path.
            if (col->is_nullable()) {
                down_cast<NullableColumn*>(col)->null_column_data().emplace_back(0);
            }
            auto* data_column = ColumnHelper::get_data_column(col);
            if (!data_column->is_struct()) {
                return Status::InternalError("expected StructColumn for STRUCT slot in append_jvalue");
            }
            auto* struct_col = down_cast<StructColumn*>(data_column);
            int num_fields = static_cast<int>(struct_col->fields_size());
            if (static_cast<int>(type_desc.children.size()) != num_fields) {
                return Status::InternalError("STRUCT append_jvalue: SQL field count != column field count");
            }

            // Look up record component accessors via reflection on Class<?>.
            // The record instance's runtime class is the formal record type the FE
            // analyzer bound to this STRUCT slot; getRecordComponents lives on Class.
            JNIEnv* env = helper.getEnv();
            jclass record_class = env->GetObjectClass(val.l);
            DeferOp drop_record_class([&]() { env->DeleteLocalRef(record_class); });
            jclass class_clazz = env->FindClass("java/lang/Class");
            DeferOp drop_class_clazz([&]() { env->DeleteLocalRef(class_clazz); });
            jmethodID get_components =
                    env->GetMethodID(class_clazz, "getRecordComponents", "()[Ljava/lang/reflect/RecordComponent;");
            if (get_components == nullptr) {
                env->ExceptionClear();
                return Status::InternalError("STRUCT append_jvalue requires Java records (JDK 14+)");
            }
            jobjectArray comps = (jobjectArray)env->CallObjectMethod(record_class, get_components);
            if (env->ExceptionCheck() || comps == nullptr) {
                env->ExceptionClear();
                return Status::InternalError("getRecordComponents failed in append_jvalue");
            }
            DeferOp drop_comps([&]() { env->DeleteLocalRef(comps); });
            jclass rc_clazz = env->FindClass("java/lang/reflect/RecordComponent");
            DeferOp drop_rc_clazz([&]() { env->DeleteLocalRef(rc_clazz); });
            jmethodID get_accessor = env->GetMethodID(rc_clazz, "getAccessor", "()Ljava/lang/reflect/Method;");
            jclass method_clazz = env->FindClass("java/lang/reflect/Method");
            DeferOp drop_method_clazz([&]() { env->DeleteLocalRef(method_clazz); });
            jmethodID method_invoke = env->GetMethodID(method_clazz, "invoke",
                                                       "(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;");

            for (int f = 0; f < num_fields; ++f) {
                jobject comp = env->GetObjectArrayElement(comps, f);
                DeferOp drop_comp([&]() { env->DeleteLocalRef(comp); });
                jobject accessor = env->CallObjectMethod(comp, get_accessor);
                DeferOp drop_accessor([&]() {
                    if (accessor) env->DeleteLocalRef(accessor);
                });
                if (env->ExceptionCheck() || accessor == nullptr) {
                    env->ExceptionClear();
                    return Status::InternalError("RecordComponent.getAccessor returned null");
                }
                jobject sub = env->CallObjectMethod(accessor, method_invoke, val.l, nullptr);
                DeferOp drop_sub([&]() {
                    if (sub) env->DeleteLocalRef(sub);
                });
                if (env->ExceptionCheck()) {
                    env->ExceptionClear();
                    return Status::InternalError("Method.invoke on record accessor threw");
                }
                jobject child_desc = get_type_desc_child(helper, type_desc_obj, f);
                DeferOp drop_child_desc([&]() {
                    if (child_desc) env->DeleteLocalRef(child_desc);
                });
                RETURN_IF_ERROR(append_jvalue(type_desc.children[f], true, struct_col->field_column_raw_ptr(f),
                                              {.l = sub}, error_if_overflow, child_desc));
            }
            break;
        }
        default:
            DCHECK(false) << "unsupport UDF TYPE" << type_desc.type;
            return Status::NotSupported(fmt::format("unsupport UDF TYPE:{}", type_desc.type));
        }
    }
    return Status::OK();
}

Status check_type_matched(const TypeDescriptor& type_desc, jobject val, jobject type_desc_obj) {
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
    case TYPE_STRUCT: {
        // The FE analyzer already validates the formal record class matches the SQL
        // STRUCT shape at CREATE FUNCTION time, so per-row class identity is enough.
        // We accept any record subclass of the declared formal class.
        if (type_desc_obj == nullptr) {
            return Status::InternalError("STRUCT check_type_matched requires UdfTypeDesc with formal record class");
        }
        jclass record_class = get_type_desc_record_class(helper, type_desc_obj);
        if (record_class == nullptr) {
            return Status::InternalError("STRUCT check_type_matched: missing formal record class");
        }
        DeferOp drop_record_class([&]() { env->DeleteLocalRef(record_class); });
        if (!env->IsInstanceOf(val, record_class)) {
            auto clazz = env->GetObjectClass(val);
            LOCAL_REF_GUARD(clazz);
            return Status::InternalError(fmt::format("Type not matched, expect record {}, but got {}",
                                                     helper.to_string(record_class), helper.to_string(clazz)));
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

// Helpers for walking a Java reflective Type tree via JNI. ParameterizedType /
// RecordComponent are introspected lazily by FindClass / GetMethodID per call;
// these are only invoked at UDF context construction, never on the per-row
// boxing hot path.
namespace {

// Resolve a formal Java reflective Type to its raw Class<?>. Accepts either a
// java.lang.Class (returned as-is) or a java.lang.reflect.ParameterizedType
// (raw type extracted via getRawType). Returns null for wildcards / type
// variables, which the FE analyzer already rejects in the field-type checker.
StatusOr<jclass> type_to_raw_class(JNIEnv* env, jobject formal_type) {
    if (formal_type == nullptr) {
        return Status::InternalError("formal Java type is null");
    }
    jclass class_clazz = env->FindClass("java/lang/Class");
    LOCAL_REF_GUARD_ENV(env, class_clazz);
    if (env->IsInstanceOf(formal_type, class_clazz)) {
        return reinterpret_cast<jclass>(formal_type);
    }
    jclass pt_clazz = env->FindClass("java/lang/reflect/ParameterizedType");
    LOCAL_REF_GUARD_ENV(env, pt_clazz);
    if (env->IsInstanceOf(formal_type, pt_clazz)) {
        jmethodID get_raw = env->GetMethodID(pt_clazz, "getRawType", "()Ljava/lang/reflect/Type;");
        jobject raw = env->CallObjectMethod(formal_type, get_raw);
        if (env->ExceptionCheck() || raw == nullptr) {
            env->ExceptionClear();
            return Status::InternalError("ParameterizedType.getRawType returned null");
        }
        return reinterpret_cast<jclass>(raw);
    }
    return Status::InternalError("formal Java type is neither Class nor ParameterizedType");
}

// Return the actual type arguments of a ParameterizedType (e.g. List<E> -> [E];
// Map<K,V> -> [K,V]). Caller is responsible for DeleteLocalRef on the returned
// array.
StatusOr<jobjectArray> type_actual_args(JNIEnv* env, jobject parameterized_type) {
    jclass pt_clazz = env->FindClass("java/lang/reflect/ParameterizedType");
    LOCAL_REF_GUARD_ENV(env, pt_clazz);
    if (!env->IsInstanceOf(parameterized_type, pt_clazz)) {
        return Status::InternalError("expected ParameterizedType for ARRAY/MAP slot");
    }
    jmethodID get_args = env->GetMethodID(pt_clazz, "getActualTypeArguments", "()[Ljava/lang/reflect/Type;");
    jobjectArray args = (jobjectArray)env->CallObjectMethod(parameterized_type, get_args);
    if (env->ExceptionCheck() || args == nullptr) {
        env->ExceptionClear();
        return Status::InternalError("ParameterizedType.getActualTypeArguments returned null");
    }
    return args;
}

} // namespace

// Build a Java-side com.starrocks.udf.UdfTypeDesc tree mirroring the SQL
// TypeDescriptor, capturing the formal record class at every STRUCT slot. Walked
// in lockstep with the Java reflective Type so List<Inner> / Map<K,V> retain
// Inner/K/V record-class info via ParameterizedType actual arguments and
// RecordComponent.getGenericType() for nested record fields.
//
// The resulting jobject is the single source of type information shared with
// the unified Java helpers (UDFHelper.writeResult / boxing helpers); the BE
// stores it as a JavaGlobalRef per arg / return and passes it back across the
// JNI boundary verbatim.
StatusOr<jobject> build_udf_type_desc(JNIEnv* env, const TypeDescriptor& td, jobject formal_type) {
    auto& helper = JVMFunctionHelper::getInstance();
    jint logical_type = static_cast<jint>(td.type);
    jint precision = static_cast<jint>(td.precision);
    jint scale = static_cast<jint>(td.scale);

    auto build_children = [&](const std::vector<jobject>& child_descs) -> StatusOr<jobjectArray> {
        jobjectArray arr =
                env->NewObjectArray(static_cast<jsize>(child_descs.size()), helper.udf_type_desc_class(), nullptr);
        if (arr == nullptr) {
            return Status::InternalError("failed to allocate UdfTypeDesc[] children");
        }
        for (size_t i = 0; i < child_descs.size(); ++i) {
            env->SetObjectArrayElement(arr, static_cast<jsize>(i), child_descs[i]);
        }
        return arr;
    };

    switch (td.type) {
    case TYPE_STRUCT: {
        ASSIGN_OR_RETURN(jclass raw, type_to_raw_class(env, formal_type));
        // Drill into record components using their parameterized generic types so
        // fields like List<Inner> retain Inner.class.
        jclass class_clazz = env->FindClass("java/lang/Class");
        LOCAL_REF_GUARD_ENV(env, class_clazz);
        jmethodID get_components =
                env->GetMethodID(class_clazz, "getRecordComponents", "()[Ljava/lang/reflect/RecordComponent;");
        jobjectArray comps = (jobjectArray)env->CallObjectMethod(raw, get_components);
        if (env->ExceptionCheck() || comps == nullptr) {
            env->ExceptionClear();
            return Status::InternalError("getRecordComponents returned null on STRUCT slot");
        }
        LOCAL_REF_GUARD_ENV(env, comps);
        jclass rc_clazz = env->FindClass("java/lang/reflect/RecordComponent");
        LOCAL_REF_GUARD_ENV(env, rc_clazz);
        jmethodID get_generic = env->GetMethodID(rc_clazz, "getGenericType", "()Ljava/lang/reflect/Type;");

        std::vector<jobject> children;
        children.reserve(td.children.size());
        for (size_t f = 0; f < td.children.size(); ++f) {
            jobject comp = env->GetObjectArrayElement(comps, static_cast<jsize>(f));
            LOCAL_REF_GUARD_ENV(env, comp);
            jobject child_formal = env->CallObjectMethod(comp, get_generic);
            if (env->ExceptionCheck() || child_formal == nullptr) {
                env->ExceptionClear();
                return Status::InternalError(fmt::format("RecordComponent.getGenericType null at {}", f));
            }
            LOCAL_REF_GUARD_ENV(env, child_formal);
            ASSIGN_OR_RETURN(jobject child_desc, build_udf_type_desc(env, td.children[f], child_formal));
            children.emplace_back(child_desc);
        }
        ASSIGN_OR_RETURN(jobjectArray children_arr, build_children(children));
        for (jobject c : children) env->DeleteLocalRef(c);
        LOCAL_REF_GUARD_ENV(env, children_arr);
        return helper.new_udf_type_desc(logical_type, children_arr, precision, scale, raw);
    }
    case TYPE_ARRAY: {
        ASSIGN_OR_RETURN(jobjectArray args, type_actual_args(env, formal_type));
        LOCAL_REF_GUARD_ENV(env, args);
        jobject elem = env->GetObjectArrayElement(args, 0);
        LOCAL_REF_GUARD_ENV(env, elem);
        ASSIGN_OR_RETURN(jobject child_desc, build_udf_type_desc(env, td.children[0], elem));
        std::vector<jobject> children = {child_desc};
        ASSIGN_OR_RETURN(jobjectArray children_arr, build_children(children));
        env->DeleteLocalRef(child_desc);
        LOCAL_REF_GUARD_ENV(env, children_arr);
        return helper.new_udf_type_desc(logical_type, children_arr, precision, scale, nullptr);
    }
    case TYPE_MAP: {
        ASSIGN_OR_RETURN(jobjectArray args, type_actual_args(env, formal_type));
        LOCAL_REF_GUARD_ENV(env, args);
        jobject key_t = env->GetObjectArrayElement(args, 0);
        LOCAL_REF_GUARD_ENV(env, key_t);
        jobject val_t = env->GetObjectArrayElement(args, 1);
        LOCAL_REF_GUARD_ENV(env, val_t);
        ASSIGN_OR_RETURN(jobject k_child, build_udf_type_desc(env, td.children[0], key_t));
        ASSIGN_OR_RETURN(jobject v_child, build_udf_type_desc(env, td.children[1], val_t));
        std::vector<jobject> children = {k_child, v_child};
        ASSIGN_OR_RETURN(jobjectArray children_arr, build_children(children));
        env->DeleteLocalRef(k_child);
        env->DeleteLocalRef(v_child);
        LOCAL_REF_GUARD_ENV(env, children_arr);
        return helper.new_udf_type_desc(logical_type, children_arr, precision, scale, nullptr);
    }
    default:
        // Scalar / decimal — leaf, no children.
        return helper.new_udf_type_desc(logical_type, nullptr, precision, scale, nullptr);
    }
}

// Helpers that read fields off a com.starrocks.udf.UdfTypeDesc jobject using the
// jfieldIDs cached at JVMFunctionHelper init time. The UDF context holds one
// UdfTypeDesc per arg / return — the input boxing path drills into it on demand
// so STRUCT slots can recover the formal Java record class without a parallel
// C++ tree.
static jobject get_type_desc_child(JVMFunctionHelper& helper, jobject type_desc, int index) {
    if (type_desc == nullptr) {
        return nullptr;
    }
    JNIEnv* env = helper.getEnv();
    jobjectArray children = (jobjectArray)env->GetObjectField(type_desc, helper.udf_type_desc_children_field());
    if (children == nullptr) {
        return nullptr;
    }
    LOCAL_REF_GUARD_ENV(env, children);
    return env->GetObjectArrayElement(children, index);
}

static jclass get_type_desc_record_class(JVMFunctionHelper& helper, jobject type_desc) {
    if (type_desc == nullptr) {
        return nullptr;
    }
    JNIEnv* env = helper.getEnv();
    return reinterpret_cast<jclass>(env->GetObjectField(type_desc, helper.udf_type_desc_record_class_field()));
}

// RAII wrapper around JNI's PushLocalFrame / PopLocalFrame. Each recursion
// level of the input boxing path pushes its own bounded frame at entry and
// pops it at exit, returning the result jobject as a fresh local ref in the
// caller's frame. Without this, deeply nested STRUCT / ARRAY<STRUCT> /
// MAP<*,STRUCT> signatures accumulate ~5-8 local refs per level (field
// arrays, sub-results, child UdfTypeDescs, ...) into the single top-level
// frame allocated in JavaFunctionCallExpr::call, eventually exhausting it.
//
// Per-level capacity is sized for the widest STRUCT we expect plus headroom
// for the recursion-internal locals; PushLocalFrame guarantees at least the
// requested capacity so JNI implementations are free to grow if needed.
class JniLocalFrame {
public:
    JniLocalFrame(JNIEnv* env, jint capacity) : _env(env) {
        if (_env->PushLocalFrame(capacity) < 0) {
            _pushed = false;
        }
    }
    ~JniLocalFrame() {
        if (_pushed && !_popped) {
            _env->PopLocalFrame(nullptr);
        }
    }
    JniLocalFrame(const JniLocalFrame&) = delete;
    JniLocalFrame& operator=(const JniLocalFrame&) = delete;

    bool ok() const { return _pushed; }

    // Pop the frame and lift `inner_result` (a local ref in this frame) into
    // the caller's frame as a fresh local ref. Returns the caller-frame ref
    // (or null if `inner_result` was null).
    jobject pop(jobject inner_result) {
        _popped = true;
        return _env->PopLocalFrame(inner_result);
    }

private:
    JNIEnv* _env;
    bool _pushed = true;
    bool _popped = false;
};

// Headroom for the small fixed set of local refs each boxer holds outside the
// per-field loop (record class, parent null buffer wrapper, offsets wrapper,
// child UdfTypeDescs, intermediate jobjectArrays). 32 covers comfortably.
static constexpr jint BOXING_FRAME_HEADROOM = 32;

// Forward declarations for the mutually recursive boxing path: STRUCT slots
// drive recursion through their fields; ARRAY/MAP slots drive recursion through
// their element / key+value children. The UdfTypeDesc jobject supplies the
// formal record class at every STRUCT slot.
static StatusOr<jobject> box_column(JVMFunctionHelper& helper, const TypeDescriptor& type_desc, const Column* column,
                                    jobject type_desc_obj, int num_rows);

static StatusOr<jobject> build_list_boxed_array(JVMFunctionHelper& helper, const TypeDescriptor& type_desc,
                                                const Column* column, jobject type_desc_obj, int num_rows) {
    JNIEnv* env = helper.getEnv();
    JniLocalFrame frame(env, BOXING_FRAME_HEADROOM);
    if (!frame.ok()) {
        return Status::InternalError("failed to push JNI local frame for ARRAY boxing");
    }

    const Column* data_column = column;
    std::unique_ptr<DirectByteBuffer> parent_null_buf;
    if (column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(column);
        const auto& null_data = nullable->immutable_null_column_data();
        parent_null_buf = std::make_unique<DirectByteBuffer>((void*)null_data.data(), null_data.size());
        data_column = nullable->data_column().get();
    }
    const auto* array_col = down_cast<const ArrayColumn*>(data_column);
    auto offsets_buf =
            std::make_unique<DirectByteBuffer>((void*)array_col->offsets().immutable_data().data(),
                                               array_col->offsets().immutable_data().size() * sizeof(uint32_t));

    const Column* elements_col = array_col->elements_column().get();
    int total_elements = static_cast<int>(elements_col->size());
    jobject elem_desc = get_type_desc_child(helper, type_desc_obj, 0);
    ASSIGN_OR_RETURN(jobject elements_obj,
                     box_column(helper, type_desc.children[0], elements_col, elem_desc, total_elements));

    const auto& method_map = helper.method_map();
    auto iter = method_map.find(TYPE_ARRAY_METHOD_ID);
    if (iter == method_map.end()) {
        return Status::NotSupported("createBoxedListArray method not registered");
    }
    jobject parent_null_handle = parent_null_buf ? parent_null_buf->handle() : nullptr;
    ASSIGN_OR_RETURN(jobject result, helper.invoke_static_method(iter->second, num_rows, parent_null_handle,
                                                                 offsets_buf->handle(), elements_obj));
    return frame.pop(result);
}

static StatusOr<jobject> build_map_boxed_array(JVMFunctionHelper& helper, const TypeDescriptor& type_desc,
                                               const Column* column, jobject type_desc_obj, int num_rows) {
    JNIEnv* env = helper.getEnv();
    JniLocalFrame frame(env, BOXING_FRAME_HEADROOM);
    if (!frame.ok()) {
        return Status::InternalError("failed to push JNI local frame for MAP boxing");
    }

    const Column* data_column = column;
    std::unique_ptr<DirectByteBuffer> parent_null_buf;
    if (column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(column);
        const auto& null_data = nullable->immutable_null_column_data();
        parent_null_buf = std::make_unique<DirectByteBuffer>((void*)null_data.data(), null_data.size());
        data_column = nullable->data_column().get();
    }
    const auto* map_col = down_cast<const MapColumn*>(data_column);
    auto offsets_buf =
            std::make_unique<DirectByteBuffer>((void*)map_col->offsets().immutable_data().data(),
                                               map_col->offsets().immutable_data().size() * sizeof(uint32_t));

    const Column* keys_col = map_col->keys_column().get();
    const Column* values_col = map_col->values_column().get();
    int total_elements = static_cast<int>(keys_col->size());

    jobject key_desc = get_type_desc_child(helper, type_desc_obj, 0);
    jobject val_desc = get_type_desc_child(helper, type_desc_obj, 1);

    ASSIGN_OR_RETURN(jobject keys_obj, box_column(helper, type_desc.children[0], keys_col, key_desc, total_elements));
    ASSIGN_OR_RETURN(jobject values_obj,
                     box_column(helper, type_desc.children[1], values_col, val_desc, total_elements));

    const auto& method_map = helper.method_map();
    auto iter = method_map.find(TYPE_MAP_METHOD_ID);
    if (iter == method_map.end()) {
        return Status::NotSupported("createBoxedMapArray method not registered");
    }
    jobject parent_null_handle = parent_null_buf ? parent_null_buf->handle() : nullptr;
    ASSIGN_OR_RETURN(jobject result, helper.invoke_static_method(iter->second, num_rows, parent_null_handle,
                                                                 offsets_buf->handle(), keys_obj, values_obj));
    return frame.pop(result);
}

static StatusOr<jobject> build_struct_boxed_array(JVMFunctionHelper& helper, const TypeDescriptor& type_desc,
                                                  const Column* column, jobject type_desc_obj, int num_rows) {
    JNIEnv* env = helper.getEnv();
    // Capacity sized for the per-level fixed locals plus one in-flight sub_result;
    // sub_results are explicitly DeleteLocalRef'd inside the per-field loop so the
    // frame footprint stays bounded regardless of field count.
    JniLocalFrame frame(env, BOXING_FRAME_HEADROOM);
    if (!frame.ok()) {
        return Status::InternalError("failed to push JNI local frame for STRUCT boxing");
    }

    jclass record_class = get_type_desc_record_class(helper, type_desc_obj);
    if (record_class == nullptr) {
        return Status::InternalError("STRUCT argument missing formal record class; cannot box");
    }

    const Column* data_column = column;
    std::unique_ptr<DirectByteBuffer> parent_null_buf;
    if (column->is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(column);
        const auto& null_data = nullable->immutable_null_column_data();
        parent_null_buf = std::make_unique<DirectByteBuffer>((void*)null_data.data(), null_data.size());
        data_column = nullable->data_column().get();
    }

    if (!data_column->is_struct()) {
        return Status::InternalError("expected StructColumn for STRUCT argument");
    }
    const auto* struct_col = down_cast<const StructColumn*>(data_column);
    int num_fields = static_cast<int>(struct_col->fields_size());

    jclass object_clazz = env->FindClass("java/lang/Object");
    jobjectArray field_arrays = env->NewObjectArray(num_fields, object_clazz, nullptr);
    if (field_arrays == nullptr) {
        return Status::InternalError("failed to allocate field_arrays for STRUCT input");
    }

    for (int f = 0; f < num_fields; ++f) {
        const Column* field_col = struct_col->field_column_raw_ptr(f);
        const TypeDescriptor& field_type = type_desc.children[f];
        jobject field_desc = get_type_desc_child(helper, type_desc_obj, f);
        ASSIGN_OR_RETURN(jobject sub_result, box_column(helper, field_type, field_col, field_desc, num_rows));
        env->SetObjectArrayElement(field_arrays, f, sub_result);
        if (sub_result != nullptr) {
            env->DeleteLocalRef(sub_result);
        }
        if (field_desc != nullptr) {
            env->DeleteLocalRef(field_desc);
        }
    }

    jobject parent_null_handle = parent_null_buf ? parent_null_buf->handle() : nullptr;
    ASSIGN_OR_RETURN(jobject result,
                     helper.create_boxed_struct_array(record_class, num_rows, parent_null_handle, field_arrays));
    return frame.pop(result);
}

// Recursive boxing dispatcher. Falls back to JavaArrayConverter when the type
// subtree contains no STRUCT (signaled by a null UdfTypeDesc), since DECIMAL
// columns and ARRAY/MAP-of-scalar already have efficient direct boxing helpers.
static StatusOr<jobject> box_column(JVMFunctionHelper& helper, const TypeDescriptor& type_desc, const Column* column,
                                    jobject type_desc_obj, int num_rows) {
    if (type_desc.type == TYPE_STRUCT) {
        return build_struct_boxed_array(helper, type_desc, column, type_desc_obj, num_rows);
    }
    if (type_desc.type == TYPE_ARRAY && type_desc_obj != nullptr) {
        return build_list_boxed_array(helper, type_desc, column, type_desc_obj, num_rows);
    }
    if (type_desc.type == TYPE_MAP && type_desc_obj != nullptr) {
        return build_map_boxed_array(helper, type_desc, column, type_desc_obj, num_rows);
    }
    if (is_decimalv3_field_type(type_desc.type)) {
        return build_decimal_boxed_array(type_desc, column, num_rows);
    }
    JavaArrayConverter conv(helper);
    RETURN_IF_ERROR(column->accept(&conv));
    return conv.result();
}

// Walk the SQL type tree to determine whether a STRUCT exists anywhere in it.
// Used to short-circuit the per-arg / return UdfTypeDesc construction when no
// STRUCT is reachable from the slot.
static bool type_subtree_has_struct(const TypeDescriptor& td) {
    if (td.type == TYPE_STRUCT) return true;
    for (const auto& c : td.children) {
        if (type_subtree_has_struct(c)) return true;
    }
    return false;
}

StatusOr<JavaUdfMethodTypeDescs> build_method_udf_type_descs(JNIEnv* env, jobject method_obj,
                                                             const std::vector<TypeDescriptor>& sql_arg_types,
                                                             const TypeDescriptor& sql_return_type, int state_offset,
                                                             bool unwrap_return_array_layer) {
    JavaUdfMethodTypeDescs out;
    int num_sql_args = static_cast<int>(sql_arg_types.size());
    out.args.reserve(num_sql_args);
    for (int i = 0; i < num_sql_args; ++i) {
        out.args.emplace_back(JavaGlobalRef(nullptr));
    }

    bool any_struct = type_subtree_has_struct(sql_return_type);
    for (int i = 0; i < num_sql_args && !any_struct; ++i) {
        if (type_subtree_has_struct(sql_arg_types[i])) {
            any_struct = true;
        }
    }
    if (!any_struct) {
        return out;
    }

    jclass method_class = env->FindClass("java/lang/reflect/Method");
    DCHECK(method_class != nullptr);
    DeferOp drop_method_class([&]() { env->DeleteLocalRef(method_class); });

    jmethodID get_generic_param =
            env->GetMethodID(method_class, "getGenericParameterTypes", "()[Ljava/lang/reflect/Type;");
    jmethodID get_generic_return = env->GetMethodID(method_class, "getGenericReturnType", "()Ljava/lang/reflect/Type;");
    jmethodID is_var_args_mid = env->GetMethodID(method_class, "isVarArgs", "()Z");
    jmethodID get_param_count_mid = env->GetMethodID(method_class, "getParameterCount", "()I");
    DCHECK(get_generic_param != nullptr && get_generic_return != nullptr);
    DCHECK(is_var_args_mid != nullptr && get_param_count_mid != nullptr);

    jobjectArray generic_params = (jobjectArray)env->CallObjectMethod(method_obj, get_generic_param);
    if (env->ExceptionCheck() || generic_params == nullptr) {
        env->ExceptionClear();
        return Status::InternalError("failed to introspect UDF method generic parameter types");
    }
    DeferOp drop_generic_params([&]() { env->DeleteLocalRef(generic_params); });

    // Java method parameter layout: [state_offset opaque slots][SQL fixed slots][optional varargs slot]
    // Varargs slot's formal type is either Class<T[]> or GenericArrayType(T<...>[]).
    jboolean is_varargs = env->CallBooleanMethod(method_obj, is_var_args_mid);
    jint java_param_count = env->CallIntMethod(method_obj, get_param_count_mid);
    int sql_fixed_count = std::max(0, java_param_count - state_offset - (is_varargs ? 1 : 0));

    jobject varargs_elem_formal = nullptr;
    DeferOp drop_varargs_elem([&]() {
        if (varargs_elem_formal) env->DeleteLocalRef(varargs_elem_formal);
    });
    if (is_varargs && num_sql_args > sql_fixed_count) {
        jobject varargs_formal = env->GetObjectArrayElement(generic_params, state_offset + sql_fixed_count);
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
            jmethodID get_component =
                    env->GetMethodID(gat_clazz, "getGenericComponentType", "()Ljava/lang/reflect/Type;");
            varargs_elem_formal = env->CallObjectMethod(varargs_formal, get_component);
        } else if (class_clazz != nullptr && env->IsInstanceOf(varargs_formal, class_clazz)) {
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

    for (int i = 0; i < num_sql_args; ++i) {
        if (!type_subtree_has_struct(sql_arg_types[i])) {
            continue;
        }
        jobject formal = nullptr;
        DeferOp drop_formal([&]() {
            if (formal) env->DeleteLocalRef(formal);
        });
        if (i < sql_fixed_count) {
            formal = env->GetObjectArrayElement(generic_params, state_offset + i);
            if (env->ExceptionCheck() || formal == nullptr) {
                env->ExceptionClear();
                return Status::InternalError(fmt::format("UDF method parameter {} formal type is null", i));
            }
        } else {
            formal = env->NewLocalRef(varargs_elem_formal);
            if (formal == nullptr) {
                return Status::InternalError("failed to retain UDF varargs element formal type");
            }
        }
        ASSIGN_OR_RETURN(jobject local_desc, build_udf_type_desc(env, sql_arg_types[i], formal));
        out.args[i] = JavaGlobalRef(env->NewGlobalRef(local_desc));
        env->DeleteLocalRef(local_desc);
    }

    if (type_subtree_has_struct(sql_return_type)) {
        jobject ret_formal = env->CallObjectMethod(method_obj, get_generic_return);
        if (env->ExceptionCheck() || ret_formal == nullptr) {
            env->ExceptionClear();
            return Status::InternalError("failed to introspect UDF method generic return type");
        }
        DeferOp drop_ret([&]() {
            if (ret_formal) env->DeleteLocalRef(ret_formal);
        });

        if (unwrap_return_array_layer) {
            // UDTF: SQL return type is the per-row element, but the Java method returns
            // `T[]` (zero or more rows per call). Drop the array layer once before pairing
            // the formal type with the SQL type — otherwise getRecordComponents() on a
            // STRUCT-of-record return would fire on `Class<Record[]>` (an array class) and
            // come back null, since array classes are not records.
            jclass gat_clazz = env->FindClass("java/lang/reflect/GenericArrayType");
            DeferOp drop_gat_clazz([&]() {
                if (gat_clazz) env->DeleteLocalRef(gat_clazz);
            });
            jclass class_clazz = env->FindClass("java/lang/Class");
            DeferOp drop_class_clazz([&]() {
                if (class_clazz) env->DeleteLocalRef(class_clazz);
            });
            jobject element_formal = nullptr;
            if (gat_clazz != nullptr && env->IsInstanceOf(ret_formal, gat_clazz)) {
                jmethodID get_component =
                        env->GetMethodID(gat_clazz, "getGenericComponentType", "()Ljava/lang/reflect/Type;");
                element_formal = env->CallObjectMethod(ret_formal, get_component);
            } else if (class_clazz != nullptr && env->IsInstanceOf(ret_formal, class_clazz)) {
                jmethodID get_component = env->GetMethodID(class_clazz, "getComponentType", "()Ljava/lang/Class;");
                element_formal = env->CallObjectMethod(ret_formal, get_component);
            } else {
                return Status::InternalError("UDTF return formal type is neither Class nor GenericArrayType");
            }
            if (env->ExceptionCheck() || element_formal == nullptr) {
                env->ExceptionClear();
                return Status::InternalError("failed to unwrap UDTF return array element type");
            }
            // Swap ret_formal for the unwrapped element type. Drop the original; the
            // DeferOp above guards `ret_formal` so reassign and delete the old one explicitly.
            env->DeleteLocalRef(ret_formal);
            ret_formal = element_formal;
        }

        ASSIGN_OR_RETURN(jobject local_ret_desc, build_udf_type_desc(env, sql_return_type, ret_formal));
        out.ret = JavaGlobalRef(env->NewGlobalRef(local_ret_desc));
        env->DeleteLocalRef(local_ret_desc);
    }

    return out;
}

Status JavaDataTypeConverter::convert_to_boxed_array(FunctionContext* ctx, const Column** columns, int num_cols,
                                                     int num_rows, std::vector<jobject>* res,
                                                     const std::vector<jobject>* arg_type_descs) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();
    for (int i = 0; i < num_cols; ++i) {
        jobject arg = nullptr;
        const TypeDescriptor& arg_type = *ctx->get_arg_type(i);
        jobject type_desc_obj = (arg_type_descs != nullptr && i < static_cast<int>(arg_type_descs->size()))
                                        ? (*arg_type_descs)[i]
                                        : nullptr;
        if (columns[i]->only_null() ||
            (columns[i]->is_nullable() && down_cast<const NullableColumn*>(columns[i])->null_count() == num_rows)) {
            arg = helper.create_array(num_rows);
        } else if (columns[i]->is_constant()) {
            auto* data_column = down_cast<const ConstColumn*>(columns[i])->data_column_raw_ptr();
            data_column->as_mutable_raw_ptr()->resize(1);
            ASSIGN_OR_RETURN(jvalue jval, cast_to_jvalue(arg_type, true, data_column, 0, type_desc_obj));
            arg = helper.create_object_array(jval.l, num_rows);
            env->DeleteLocalRef(jval.l);
        } else {
            ASSIGN_OR_RETURN(arg, box_column(helper, arg_type, columns[i], type_desc_obj, num_rows));
        }

        res->emplace_back(arg);
    }
    return Status::OK();
}
} // namespace starrocks
