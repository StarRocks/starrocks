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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/raw_data_visitor.h"
#include "column/vectorized_fwd.h"
#include "exprs/base64.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "types/type_descriptor.h"
#include "udf/java/java_udf.h"

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
        RawDataVisitor rv;
        ASSERT_OK(data_column->accept(&rv));
        ASSERT_EQ(results[0], (jlong)nullable_column->null_column_data().data());
        ASSERT_EQ(results[1], (jlong)rv.result());
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
        ASSERT_EQ(results[2], (jlong)binary_column->get_immutable_bytes().data());
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
        ASSERT_EQ(results[1], (jlong)array_column->offsets().immutable_data().data());
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
        ASSERT_EQ(results[1], (jlong)map_column->offsets().immutable_data().data());
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
    // ARRAY and MAP must be reachable: the recursive output path on the Java side
    // (UDFHelper.getResultFromListArray / getResultFromMapArray) calls back into
    // getColumnLogicalType on element/key/value columns to dispatch nested writes.
    {
        TypeDescriptor array_type(TYPE_ARRAY);
        array_type.children.emplace_back(TYPE_INT);
        auto c = ColumnHelper::create_column(array_type, true);
        ASSERT_EQ(TYPE_ARRAY, JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(c.get())));
    }
    {
        TypeDescriptor map_type(TYPE_MAP);
        map_type.children.emplace_back(TYPE_INT);
        map_type.children.emplace_back(TYPE_VARCHAR);
        auto c = ColumnHelper::create_column(map_type, true);
        ASSERT_EQ(TYPE_MAP, JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(c.get())));
    }
}

// DECIMAL columns are FixedLengthColumnBase but not FixedLengthColumn, so the
// generic visitor overload doesn't match. The dedicated DECIMAL specialization
// below has to map int32/int64/int128/int256 -> DECIMAL32/64/128/256.
TEST_F(JavaNativeMethodTest, get_column_logical_type_decimal) {
    auto env = getJNIEnv();
    struct Case {
        LogicalType type;
        int precision;
        int scale;
    };
    std::vector<Case> decimal_cases = {
            {TYPE_DECIMAL32, 9, 2},
            {TYPE_DECIMAL64, 18, 4},
            {TYPE_DECIMAL128, 38, 10},
            {TYPE_DECIMAL256, 76, 10},
    };
    for (const auto& c : decimal_cases) {
        auto desc = TypeDescriptor::create_decimalv3_type(c.type, c.precision, c.scale);
        auto c1 = ColumnHelper::create_column(desc, true);
        auto c2 = ColumnHelper::create_column(desc, false);
        auto logical_type_1 = JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(c1.get()));
        auto logical_type_2 = JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(c2.get()));
        ASSERT_EQ(c.type, logical_type_1);
        ASSERT_EQ(c.type, logical_type_2);
    }
}

// DateColumn / TimestampColumn are FixedLengthColumn<DateValue> and
// FixedLengthColumn<TimestampValue>. Without explicit handling in the visitor's
// FixedLengthColumn<T> template, the call falls through to "unsupported UDF
// type", which is the bug fixed in this commit. Make sure both shapes resolve.
TEST_F(JavaNativeMethodTest, get_column_logical_type_date_datetime) {
    auto env = getJNIEnv();
    for (auto type : {TYPE_DATE, TYPE_DATETIME}) {
        for (bool nullable : {true, false}) {
            auto col = ColumnHelper::create_column(TypeDescriptor(type), nullable);
            auto t = JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(col.get()));
            EXPECT_EQ(type, t) << "type=" << type << " nullable=" << nullable;
        }
    }
}

// ARRAY<DATE> / ARRAY<DATETIME>: the Java side recurses into the element column
// to dispatch the per-row write, so the element column must also resolve.
TEST_F(JavaNativeMethodTest, get_column_logical_type_array_of_date_datetime) {
    auto env = getJNIEnv();
    for (auto type : {TYPE_DATE, TYPE_DATETIME}) {
        TypeDescriptor array_type(TYPE_ARRAY);
        array_type.children.emplace_back(type);
        auto col = ColumnHelper::create_column(array_type, true);
        EXPECT_EQ(TYPE_ARRAY,
                  JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(col.get())));

        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        const auto* array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
        const auto* elements = array_col->elements_column().get();
        EXPECT_EQ(type, JavaNativeMethods::getColumnLogicalType(env, nullptr, reinterpret_cast<size_t>(elements)))
                << "elements of ARRAY<" << type << ">";
    }
}

// getAddrs on a DATE / DATETIME column returns the underlying int32 / int64 raw
// buffer that the Java helper memcpys into. Verify the data pointer matches the
// FixedLengthColumn raw storage.
TEST_F(JavaNativeMethodTest, get_addrs_date_datetime) {
    auto env = getJNIEnv();
    {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
        col->resize(8);
        auto arr = JavaNativeMethods::getAddrs(env, nullptr, reinterpret_cast<size_t>(col.get()));
        ASSERT_NE(arr, nullptr);
        jlong results[4];
        env->GetLongArrayRegion(arr, 0, 4, results);
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        const auto* date_col = down_cast<const DateColumn*>(nullable->data_column().get());
        EXPECT_EQ(results[0], (jlong)nullable->null_column_data().data());
        EXPECT_EQ(results[1], (jlong)date_col->immutable_data().data());
        env->DeleteLocalRef(arr);
    }
    {
        auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
        col->resize(8);
        auto arr = JavaNativeMethods::getAddrs(env, nullptr, reinterpret_cast<size_t>(col.get()));
        ASSERT_NE(arr, nullptr);
        jlong results[4];
        env->GetLongArrayRegion(arr, 0, 4, results);
        const auto* nullable = down_cast<const NullableColumn*>(col.get());
        const auto* ts_col = down_cast<const TimestampColumn*>(nullable->data_column().get());
        EXPECT_EQ(results[0], (jlong)nullable->null_column_data().data());
        EXPECT_EQ(results[1], (jlong)ts_col->immutable_data().data());
        env->DeleteLocalRef(arr);
    }
}

// getAddrs on DECIMAL columns must hand back data, precision, scale (in addition to
// the null buffer), matching the DECIMAL-aware specialization in GetColumnAddrVisitor.
TEST_F(JavaNativeMethodTest, get_addrs_decimal) {
    auto env = getJNIEnv();
    struct Case {
        LogicalType type;
        int precision;
        int scale;
    };
    std::vector<Case> decimal_cases = {
            {TYPE_DECIMAL32, 9, 2},
            {TYPE_DECIMAL64, 18, 4},
            {TYPE_DECIMAL128, 38, 10},
            {TYPE_DECIMAL256, 76, 10},
    };
    for (const auto& c : decimal_cases) {
        auto desc = TypeDescriptor::create_decimalv3_type(c.type, c.precision, c.scale);
        auto column = ColumnHelper::create_column(desc, true);
        column->resize(4);
        auto arr = JavaNativeMethods::getAddrs(env, nullptr, reinterpret_cast<size_t>(column.get()));
        ASSERT_NE(arr, nullptr);
        jlong results[4];
        env->GetLongArrayRegion(arr, 0, 4, results);
        const auto* nullable_column = down_cast<const NullableColumn*>(column.get());
        ASSERT_EQ(results[0], (jlong)nullable_column->null_column_data().data());
        ASSERT_EQ(results[2], static_cast<jlong>(c.precision));
        ASSERT_EQ(results[3], static_cast<jlong>(c.scale));
        env->DeleteLocalRef(arr);
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