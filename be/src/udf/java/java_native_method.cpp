// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "udf/java/java_native_method.h"

#include <new>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

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
    Status do_visit(const vectorized::FixedLengthColumn<T>& column) {
        _jarr[_idx++] = reinterpret_cast<int64_t>(column.get_data().data());
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const T& column) {
        return Status::NotSupported("UDF Not Support Type");
    }

private:
    size_t _idx = 0;
    jlong* _jarr = 0;
};

jlong JavaNativeMethods::resizeStringData(JNIEnv* env, jclass clazz, jlong columnAddr, jint byteSize) {
    Column* column = reinterpret_cast<Column*>(columnAddr);
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
    Column* column = reinterpret_cast<Column*>(columnAddr);
    // return fixed array size
    int array_size = 3;
    auto jarr = env->NewLongArray(array_size);
    jlong array[array_size];
    GetColumnAddrVistor vistor(array);
    column->accept(&vistor);
    env->SetLongArrayRegion(jarr, 0, array_size, array);
    return jarr;
}
} // namespace starrocks::vectorized