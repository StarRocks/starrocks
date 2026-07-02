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

#include "runtime/java/native_method_helper.h"

#include <cstdlib>
#include <exception>
#include <limits>
#include <mutex>
#include <new>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "base/logging.h"
#include "column/array_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/decimalv3_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "fmt/core.h"
#include "gutil/casts.h"
#include "runtime/java/jvm_helper.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {
namespace {

constexpr const char* kNativeMethodHelperClassName = "com/starrocks/utils/NativeMethodHelper";

class NativeMethodRegistrationError final : public std::exception {
public:
    explicit NativeMethodRegistrationError(Status status) : _status(std::move(status)), _message(_status.to_string()) {}

    const char* what() const noexcept override { return _message.c_str(); }
    const Status& status() const { return _status; }

private:
    Status _status;
    std::string _message;
};

class GetColumnAddrVistor : public ColumnVisitorAdapter<GetColumnAddrVistor> {
public:
    GetColumnAddrVistor(jlong* jarr) : ColumnVisitorAdapter(this), _jarr(jarr) {}

    Status do_visit(const NullableColumn& column) {
        const auto& null_data = column.immutable_null_column_data();
        _jarr[_idx++] = reinterpret_cast<int64_t>(null_data.data());
        return column.data_column()->accept(this);
    }

    Status do_visit(const BinaryColumn& column) {
        const auto& column_offsets = column.get_offset();
        const size_t java_max_buffer_size = std::numeric_limits<int>::max();
        if (column_offsets.is_large() || column_offsets.back() > java_max_buffer_size ||
            column.get_immutable_bytes().size() > java_max_buffer_size ||
            column_offsets.size() > java_max_buffer_size / sizeof(uint32_t)) {
            return Status::NotSupported("Java UDF does not support BinaryColumn with large offsets or bytes");
        }
        column_offsets.visit_storage(
                [&](const auto& offsets) { _jarr[_idx++] = reinterpret_cast<int64_t>(offsets.data()); });
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.get_immutable_bytes().data());
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.offsets().immutable_data().data());
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.elements_column().get());
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.offsets().immutable_data().data());
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.keys_column().get());
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.values_column().get());
        return Status::OK();
    }

    // STRUCT field count is variable so the fixed 4-slot getAddrs() return shape
    // cannot carry every subfield pointer. The Java helper UDFHelper.writeResult
    // calls getAddrs() only for the parent NullableColumn's null bitmap (already
    // populated by the NullableColumn arm above); per-subfield Column pointers
    // are exposed via the dedicated getStructFieldAddrs native helper.
    Status do_visit(const StructColumn& column) { return Status::OK(); }

    template <typename T>
    Status do_visit(const FixedLengthColumn<T>& column) {
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.immutable_data().data());
        return Status::OK();
    }

    // DecimalV3Column<T> is a sibling of FixedLengthColumn<T> (both inherit from
    // FixedLengthColumnBase), so the FixedLengthColumn overload above does not match.
    // Explicit specialization keeps DECIMAL columns reachable from native helpers like
    // getAddrs that the Java side calls for DECIMAL UDF return paths.
    //
    // Layout: addrs[0]=null, addrs[1]=data, addrs[2]=precision, addrs[3]=scale.
    // The trailing precision/scale slots are unused by other column shapes, so the Java
    // side can pull them out for DECIMAL element write-back without a separate JNI call.
    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.immutable_data().data());
        _jarr[_idx++] = static_cast<int64_t>(column.precision());
        _jarr[_idx++] = static_cast<int64_t>(column.scale());
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const T& column) {
        return Status::NotSupported("UDF Not Support Type");
    }

private:
    size_t _idx = 0;
    jlong* _jarr = nullptr;
};

class GetColumnLogicalTypeVistor : public ColumnVisitorAdapter<GetColumnLogicalTypeVistor> {
public:
    GetColumnLogicalTypeVistor(LogicalType* result) : ColumnVisitorAdapter(this), _result(result) {}

    Status do_visit(const NullableColumn& column) { return column.data_column()->accept(this); }

    Status do_visit(const BinaryColumn& column) {
        *_result = TYPE_VARCHAR;
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        *_result = TYPE_ARRAY;
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        *_result = TYPE_MAP;
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        *_result = TYPE_STRUCT;
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumn<T>& column) {
        if constexpr (std::is_same_v<T, uint8_t>) {
            *_result = TYPE_BOOLEAN;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, int8_t>) {
            *_result = TYPE_TINYINT;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, int16_t>) {
            *_result = TYPE_SMALLINT;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, int32_t>) {
            *_result = TYPE_INT;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, int64_t>) {
            *_result = TYPE_BIGINT;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, int128_t>) {
            *_result = TYPE_LARGEINT;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, float>) {
            *_result = TYPE_FLOAT;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, double>) {
            *_result = TYPE_DOUBLE;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, DateValue>) {
            *_result = TYPE_DATE;
            return Status::OK();
        } else if constexpr (std::is_same_v<T, TimestampValue>) {
            *_result = TYPE_DATETIME;
            return Status::OK();
        }
        return Status::NotSupported("unsupported UDF type");
    }

    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        if constexpr (std::is_same_v<T, int32_t>) {
            *_result = TYPE_DECIMAL32;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            *_result = TYPE_DECIMAL64;
        } else if constexpr (std::is_same_v<T, int128_t>) {
            *_result = TYPE_DECIMAL128;
        } else if constexpr (std::is_same_v<T, int256_t>) {
            *_result = TYPE_DECIMAL256;
        } else {
            return Status::NotSupported("unsupported decimal width");
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const T& column) {
        return Status::NotSupported("unsupported UDF type");
    }

private:
    LogicalType* _result;
};

jlong resize_string_data(JNIEnv* env, jclass clazz, jlong columnAddr, jint byteSize) {
    auto* column = reinterpret_cast<Column*>(columnAddr); // NOLINT
    BinaryColumn* binary_column = nullptr;
    if (column->is_nullable()) {
        binary_column =
                ColumnHelper::cast_to_raw<TYPE_VARCHAR>(down_cast<NullableColumn*>(column)->data_column_raw_ptr());
    } else {
        binary_column = down_cast<BinaryColumn*>(column);
    }

    try {
        binary_column->get_bytes().resize(byteSize);
    } catch (std::bad_alloc&) {
        binary_column->reset_column();
        env->ThrowNew(env->FindClass("java/lang/OutOfMemoryError"),
                      fmt::format("OOM try to allocate {} in java native function", byteSize).c_str());
        return 0;
    }
    return reinterpret_cast<jlong>(binary_column->get_bytes().data());
}

void resize_column(JNIEnv* env, jclass clazz, jlong columnAddr, jint size) {
    auto* column = reinterpret_cast<Column*>(columnAddr); // NOLINT
    column->resize(size);
}

jint get_column_logical_type(JNIEnv* env, jclass clazz, jlong columnAddr) {
    auto* column = reinterpret_cast<Column*>(columnAddr); // NOLINT
    LogicalType type;
    GetColumnLogicalTypeVistor vistor(&type);
    auto status = column->accept(&vistor);
    if (!status.ok()) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"), status.to_string().c_str());
    }
    return type;
}

jlongArray get_addrs(JNIEnv* env, jclass clazz, jlong columnAddr) {
    auto* column = reinterpret_cast<Column*>(columnAddr); // NOLINT
    if (!column->is_nullable()) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"),
                      fmt::format("result column should be nullable column").c_str());
        return nullptr;
    }
    // return fixed array size
    constexpr int array_size = 4;
    auto jarr = env->NewLongArray(array_size);
    jlong array[array_size] = {};
    GetColumnAddrVistor vistor(array);
    if (!column->accept(&vistor).ok()) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"),
                      fmt::format("GetColumnAddr in java native function error").c_str());
        return jarr;
    }
    env->SetLongArrayRegion(jarr, 0, array_size, array);
    return jarr;
}

jlong memory_malloc(JNIEnv* env, jclass clazz, jlong bytes) {
    long address = reinterpret_cast<long>(std::malloc(bytes));
    VLOG_ROW << "Allocated " << bytes << " bytes of memory address " << address;
    return address;
}

void memory_free(JNIEnv* env, jclass clazz, jlong address) {
    VLOG_ROW << "Freed memory address " << address << ".";
    std::free(reinterpret_cast<void*>(address)); // NOLINT
}

jlongArray get_struct_field_addrs(JNIEnv* env, jclass clazz, jlong columnAddr) {
    auto* column = reinterpret_cast<Column*>(columnAddr); // NOLINT
    if (column == nullptr || !column->is_nullable()) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"),
                      "getStructFieldAddrs expects a NullableColumn");
        return nullptr;
    }
    auto* data_col = down_cast<NullableColumn*>(column)->data_column_raw_ptr();
    if (data_col == nullptr || !data_col->is_struct()) {
        env->ThrowNew(env->FindClass("java/lang/IllegalArgumentException"),
                      "getStructFieldAddrs expects a NullableColumn(StructColumn)");
        return nullptr;
    }
    auto* struct_col = down_cast<StructColumn*>(data_col);
    int n = static_cast<int>(struct_col->fields_size());
    jlongArray jarr = env->NewLongArray(n);
    std::vector<jlong> addrs(n);
    for (int i = 0; i < n; ++i) {
        addrs[i] = reinterpret_cast<jlong>(struct_col->field_column_raw_ptr(i));
    }
    if (n > 0) {
        env->SetLongArrayRegion(jarr, 0, n, addrs.data());
    }
    return jarr;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
JNINativeMethod kNativeMethods[] = {
        {"resizeStringData", "(JI)J", (void*)&resize_string_data},
        {"resize", "(JI)V", (void*)&resize_column},
        {"getColumnLogicalType", "(J)I", (void*)&get_column_logical_type},
        {"getAddrs", "(J)[J", (void*)&get_addrs},
        {"getStructFieldAddrs", "(J)[J", (void*)&get_struct_field_addrs},
        {"memoryTrackerMalloc", "(J)J", (void*)&memory_malloc},
        {"memoryTrackerFree", "(J)V", (void*)&memory_free},
};
#pragma GCC diagnostic pop

Status jni_exception_status(JNIEnv* env, const std::string& action) {
    jthrowable thr = env->ExceptionOccurred();
    if (thr == nullptr) {
        return Status::InternalError(action);
    }
    env->ExceptionClear();
    std::string message = JVMHelper::getInstance().dumpExceptionString(thr);
    env->DeleteLocalRef(thr);
    return Status::InternalError(fmt::format("{}: {}", action, message));
}

Status register_native_methods(JNIEnv* env) {
    jclass native_method_class = env->FindClass(kNativeMethodHelperClassName);
    if (native_method_class == nullptr) {
        return jni_exception_status(env, "Failed to find NativeMethodHelper class");
    }
    LOCAL_REF_GUARD_ENV(env, native_method_class);

    int res = env->RegisterNatives(native_method_class, kNativeMethods,
                                   sizeof(kNativeMethods) / sizeof(kNativeMethods[0]));
    if (res != 0) {
        if (env->ExceptionCheck()) {
            return jni_exception_status(env, "Failed to register NativeMethodHelper natives");
        }
        return Status::InternalError(
                fmt::format("Failed to register NativeMethodHelper natives, RegisterNatives returned {}", res));
    }
    return Status::OK();
}

} // namespace

Status NativeMethodHelper::ensure_registered(JNIEnv* env) {
    if (env == nullptr) {
        return Status::InternalError("JNIEnv is null when registering NativeMethodHelper");
    }

    static std::once_flag register_once;

    try {
        // RegisterNatives must run exactly once after success. If registration fails, throw from
        // the call_once body so the once_flag remains unset and a later call can retry.
        std::call_once(register_once, [&]() {
            Status status = register_native_methods(env);
            if (!status.ok()) {
                throw NativeMethodRegistrationError(std::move(status));
            }
        });
    } catch (const NativeMethodRegistrationError& e) {
        return e.status();
    }
    return Status::OK();
}

} // namespace starrocks
