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

#include "udf/java/java_native_method.h"

#include <new>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "types/logical_type.h"

namespace starrocks {

class GetColumnAddrVistor : public ColumnVisitorAdapter<GetColumnAddrVistor> {
public:
    GetColumnAddrVistor(jlong* jarr) : ColumnVisitorAdapter(this), _jarr(jarr) {}

    Status do_visit(const NullableColumn& column) {
        const auto& null_data = column.immutable_null_column_data();
        _jarr[_idx++] = reinterpret_cast<int64_t>(null_data.data());
        return column.data_column()->accept(this);
    }

    Status do_visit(const BinaryColumn& column) {
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.get_offset().data());
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.get_bytes().data());
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumn<T>& column) {
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.get_data().data());
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

jlong JavaNativeMethods::resizeStringData(JNIEnv* env, jclass clazz, jlong columnAddr, jint byteSize) {
    auto* column = reinterpret_cast<Column*>(columnAddr); // NOLINT
    BinaryColumn* binary_column = nullptr;
    if (column->is_nullable()) {
        binary_column = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(down_cast<NullableColumn*>(column)->data_column());
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

jlongArray JavaNativeMethods::getAddrs(JNIEnv* env, jclass clazz, jlong columnAddr) {
    auto* column = reinterpret_cast<Column*>(columnAddr); // NOLINT
    // return fixed array size
    int array_size = 3;
    auto jarr = env->NewLongArray(array_size);
    jlong array[array_size];
    GetColumnAddrVistor vistor(array);
    WARN_IF_ERROR(column->accept(&vistor), "get column addr failed");
    env->SetLongArrayRegion(jarr, 0, array_size, array);
    return jarr;
}

jlong JavaNativeMethods::memory_malloc(JNIEnv* env, jclass clazz, jlong bytes) {
    long address = reinterpret_cast<long>(malloc(bytes));
    VLOG_ROW << "Allocated " << bytes << " bytes of memory address " << address;
    return address;
}

void JavaNativeMethods::memory_free(JNIEnv* env, jclass clazz, jlong address) {
    VLOG_ROW << "Freed memory address " << address << ".";
    free(reinterpret_cast<void*>(address)); // NOLINT
}

} // namespace starrocks
