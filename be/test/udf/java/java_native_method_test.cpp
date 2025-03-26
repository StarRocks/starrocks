#include "udf/java/java_native_method.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/base64.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "udf/java/java_udf.h"
#include "util/defer_op.h"

namespace starrocks {

class JavaNativeMethodTest : public testing::Test {
public:
    JavaNativeMethodTest() = default;
};

TEST_F(JavaNativeMethodTest, get_addrs_int) {
    auto env = getJNIEnv();
    std::vector<LogicalType> numberic_types = {TYPE_INT, TYPE_BIGINT, TYPE_FLOAT, TYPE_DOUBLE};
    for (auto type : numberic_types) {
        TypeDescriptor desc(type);
        auto column = ColumnHelper::create_column(desc, true);
        column->resize(10);
        auto arr = JavaNativeMethods::getAddrs(env, nullptr, reinterpret_cast<size_t>(column.get()));
        jlong results[5];
        env->GetLongArrayRegion(arr, 0, 4, results);
        const NullableColumn* nullable_column = down_cast<const NullableColumn*>(column.get());
        const Column* data_column = down_cast<const NullableColumn*>(column.get())->data_column().get();
        ASSERT_EQ(results[0], (jlong)nullable_column->null_column_data().data());
        ASSERT_EQ(results[1], (jlong)data_column->raw_data());
        env->DeleteLocalRef(arr);
    }
    std::vector<LogicalType> string_types = {TYPE_CHAR, TYPE_VARCHAR};
    for (auto type : string_types) {
        TypeDescriptor desc(type);
        auto column = ColumnHelper::create_column(desc, true);
        column->resize(10);
        auto arr = JavaNativeMethods::getAddrs(env, nullptr, reinterpret_cast<size_t>(column.get()));
        jlong results[5];
        env->GetLongArrayRegion(arr, 0, 4, results);
        const NullableColumn* nullable_column = down_cast<const NullableColumn*>(column.get());
        const Column* data_column = down_cast<const NullableColumn*>(column.get())->data_column().get();
        const auto* binary_column = down_cast<const BinaryColumn*>(data_column);
        ASSERT_EQ(results[0], (jlong)nullable_column->null_column_data().data());
        ASSERT_EQ(results[1], (jlong)binary_column->get_offset().data());
        ASSERT_EQ(results[2], (jlong)binary_column->get_bytes().data());
        env->DeleteLocalRef(arr);
    }
    // test array/map
    // array with scalar types
    {
        TypeDescriptor array_type(TYPE_ARRAY);
        array_type.children.emplace_back(TYPE_INT);
        auto column = ColumnHelper::create_column(array_type, true);
        column->resize(10);
        auto arr = JavaNativeMethods::getAddrs(env, nullptr, reinterpret_cast<size_t>(column.get()));
        jlong results[5];
        env->GetLongArrayRegion(arr, 0, 4, results);
        const NullableColumn* nullable_column = down_cast<const NullableColumn*>(column.get());
        const Column* data_column = down_cast<const NullableColumn*>(column.get())->data_column().get();
        const auto* array_column = down_cast<const ArrayColumn*>(data_column);
        ASSERT_EQ(results[0], (jlong)nullable_column->null_column_data().data());
        ASSERT_EQ(results[1], (jlong)array_column->offsets().get_data().data());
        ASSERT_EQ(results[2], (jlong)array_column->elements_column().get());
        env->DeleteLocalRef(arr);
    }
    // test map
    {
        TypeDescriptor map_type(TYPE_MAP);
        map_type.children.emplace_back(TYPE_INT);
        map_type.children.emplace_back(TYPE_INT);
        auto column = ColumnHelper::create_column(map_type, true);
        column->resize(10);
        auto arr = JavaNativeMethods::getAddrs(env, nullptr, reinterpret_cast<size_t>(column.get()));
        jlong results[5];
        env->GetLongArrayRegion(arr, 0, 4, results);
        const NullableColumn* nullable_column = down_cast<const NullableColumn*>(column.get());
        const Column* data_column = nullable_column->data_column();
        auto* map_column = down_cast<const MapColumn*>(data_column);
        ASSERT_EQ(results[0], (jlong)nullable_column->null_column_data().data());
        ASSERT_EQ(results[1], (jlong)map_column->offsets().get_data().data());
        ASSERT_EQ(results[2], (jlong)map_column->keys_column().get());
        ASSERT_EQ(results[3], (jlong)map_column->values_column().get());
        env->DeleteLocalRef(arr);
    }
}

TEST_F(JavaNativeMethodTest, get_column_logical_type) {
    auto env = getJNIEnv();
    // scalar type test
    std::vector<LogicalType> scalar_types = {TYPE_SMALLINT, TYPE_INT,    TYPE_BIGINT, TYPE_LARGEINT,
                                             TYPE_FLOAT,    TYPE_DOUBLE, TYPE_VARCHAR};
    for (auto type : scalar_types) {
        auto c1 = ColumnHelper::create_column(TypeDescriptor(type), true);
        auto c2 = ColumnHelper::create_column(TypeDescriptor(type), false);
        auto logical_type_1 = JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(c1.get()));
        auto logical_type_2 = JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(c2.get()));
        ASSERT_EQ(type, logical_type_1);
        ASSERT_EQ(type, logical_type_2);
    }
}
TEST_F(JavaNativeMethodTest, resize) {
    auto env = getJNIEnv();
    // scalar type test
    std::vector<LogicalType> scalar_types = {TYPE_SMALLINT, TYPE_INT,    TYPE_BIGINT, TYPE_LARGEINT,
                                             TYPE_FLOAT,    TYPE_DOUBLE, TYPE_VARCHAR};
    for (auto type : scalar_types) {
        auto c1 = ColumnHelper::create_column(TypeDescriptor(type), true);
        JavaNativeMethods::resize(env, nullptr, reinterpret_cast<size_t>(c1.get()), 4096);
    }
    auto str = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
    JavaNativeMethods::resizeStringData(env, nullptr, (jlong)str.get(), 4096);
    auto binary_column = ColumnHelper::get_binary_column(str.get());
    ASSERT_EQ(binary_column->get_bytes().size(), 4096);
    binary_column->get_bytes().clear();
}

} // namespace starrocks