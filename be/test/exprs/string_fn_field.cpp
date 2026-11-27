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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <random>

#include "column/array_column.h"
#include "exprs/function_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/string_functions.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "types/large_int_value.h"

namespace starrocks {

PARALLEL_TEST(VecFieldFunctionsTest, fieldStringTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    for (int j = 0; j < 5; ++j) {
        str1->append("test" + std::to_string(j));
        str2->append("test" + std::to_string(j % 10));
    }

    columns.emplace_back(std::move(str1));
    columns.emplace_back(std::move(str2));

    ColumnPtr result = StringFunctions::field<TYPE_VARCHAR>(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<Int32Column>(result);
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(1, v->immutable_data()[i]);
    }
}

// FIELD(NULL, ...) is 0 (RB-1216). A NULL row of a non-constant first argument used to be compared by the value
// left under its null flag, so it returned the position of an argument equal to that value.
static ColumnPtr nullable_int_column(const std::vector<int32_t>& values, const std::vector<uint8_t>& nulls) {
    auto data = Int32Column::create();
    auto null_column = NullColumn::create();
    for (size_t i = 0; i < values.size(); i++) {
        data->append(values[i]);
        null_column->append(nulls[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null_column));
}

PARALLEL_TEST(VecFieldFunctionsTest, fieldNullFirstArgumentMatchesNothing) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    // Row 0 is NULL with 1 under the flag, row 1 is 2.
    columns.emplace_back(nullable_int_column({1, 2}, {1, 0}));
    auto arg1 = Int32Column::create();
    arg1->append(1);
    arg1->append(1);
    auto arg2 = Int32Column::create();
    arg2->append(2);
    arg2->append(2);
    columns.emplace_back(std::move(arg1));
    columns.emplace_back(std::move(arg2));

    ColumnPtr result = StringFunctions::field<TYPE_INT>(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<Int32Column>(result);
    ASSERT_EQ(0, v->immutable_data()[0]);
    ASSERT_EQ(2, v->immutable_data()[1]);
}

PARALLEL_TEST(VecFieldFunctionsTest, fieldNullFirstArgumentMatchesNoConstant) {
    const TypeDescriptor int_type(TYPE_INT);
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_test_context({int_type, int_type, int_type}, int_type));
    Columns columns;
    // Rows 0 and 2 are NULL with 3 and 1 under the flag; the other arguments are the constants 1 and 3.
    columns.emplace_back(nullable_int_column({3, 3, 1}, {1, 0, 1}));
    columns.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(1, 3));
    columns.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(3, 3));
    ctx->set_constant_columns({nullptr, columns[1], columns[2]});
    ASSERT_TRUE(StringFunctions::field_prepare<TYPE_INT>(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = StringFunctions::field<TYPE_INT>(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::field_close<TYPE_INT>(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    auto v = ColumnHelper::as_column<Int32Column>(result);
    ASSERT_EQ(0, v->immutable_data()[0]);
    ASSERT_EQ(2, v->immutable_data()[1]);
    ASSERT_EQ(0, v->immutable_data()[2]);
}

PARALLEL_TEST(VecFieldFunctionsTest, fieldIntTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto paramBuilder1 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder2 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder3 = ColumnBuilder<TYPE_INT>(5);

    paramBuilder1.append(5);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append(2);
    paramBuilder2.append(2);
    paramBuilder3.append(2);

    paramBuilder1.append(2);
    paramBuilder2.append(1);
    paramBuilder3.append(2);

    paramBuilder1.append_null();
    paramBuilder2.append(1);
    paramBuilder3.append(2);

    paramBuilder1.append_null();
    paramBuilder2.append(1);
    paramBuilder3.append_null();

    auto param1 = paramBuilder1.build_nullable_column();
    auto param2 = paramBuilder2.build_nullable_column();
    auto param3 = paramBuilder3.build_nullable_column();

    columns.emplace_back(std::move(param1));
    columns.emplace_back(std::move(param2));
    columns.emplace_back(std::move(param3));
    int res[] = {0, 1, 2, 0, 0};
    ctx->set_function_state(FunctionContext::FRAGMENT_LOCAL, nullptr);
    ColumnPtr result = StringFunctions::field<TYPE_INT>(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<Int32Column>(result);
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(res[i], v->immutable_data()[i]);
    }
}

PARALLEL_TEST(VecFieldFunctionsTest, fieldDecTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    int num_rows = 3;
    int precision = 10;
    int scale = 3;
    auto param1 = DecimalV3Column<int64_t>::create(precision, scale, num_rows);
    auto& data1 = down_cast<DecimalV3Column<int64_t>*>(param1.get())->get_data();

    auto param2 = DecimalV3Column<int64_t>::create(precision, scale, num_rows);
    auto& data2 = down_cast<DecimalV3Column<int64_t>*>(param2.get())->get_data();

    auto param3 = DecimalV3Column<int64_t>::create(precision, scale, num_rows);
    auto& data3 = down_cast<DecimalV3Column<int64_t>*>(param3.get())->get_data();

    std::string s = "";
    int i = 0;
    s = "1.1";
    DecimalV3Cast::from_string<int64_t>(&data1[i], precision, scale, s.c_str(), s.size());
    s = "1.2";
    DecimalV3Cast::from_string<int64_t>(&data2[i], precision, scale, s.c_str(), s.size());
    s = "1.2";
    DecimalV3Cast::from_string<int64_t>(&data3[i], precision, scale, s.c_str(), s.size());
    i++;
    s = "1.2";
    DecimalV3Cast::from_string<int64_t>(&data1[i], precision, scale, s.c_str(), s.size());
    s = "1.0";
    DecimalV3Cast::from_string<int64_t>(&data2[i], precision, scale, s.c_str(), s.size());
    s = "1.2";
    DecimalV3Cast::from_string<int64_t>(&data3[i], precision, scale, s.c_str(), s.size());
    i++;
    s = "1.2777";
    DecimalV3Cast::from_string<int64_t>(&data1[i], precision, scale, s.c_str(), s.size());
    s = "1.2777000";
    DecimalV3Cast::from_string<int64_t>(&data2[i], precision, scale, s.c_str(), s.size());
    s = "1.2555";
    DecimalV3Cast::from_string<int64_t>(&data3[i], precision, scale, s.c_str(), s.size());
    int res[] = {0, 2, 1};

    columns.emplace_back(std::move(param1));
    columns.emplace_back(std::move(param2));
    columns.emplace_back(std::move(param3));
    ColumnPtr result = StringFunctions::field<TYPE_DECIMAL64>(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<Int32Column>(result);
    for (int i = 0; i < num_rows; ++i) {
        ASSERT_EQ(res[i], v->immutable_data()[i]);
    }
}

PARALLEL_TEST(VecFieldFunctionsTest, fieldIntTest2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto paramBuilder1 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder2 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder3 = ColumnBuilder<TYPE_INT>(5);

    paramBuilder1.append(5);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append(2);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append(2);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append_null();
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append_null();
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    auto param1 = paramBuilder1.build_nullable_column();
    auto param2 = paramBuilder2.build(true);
    auto param3 = paramBuilder3.build(true);

    columns.emplace_back(std::move(param1));
    columns.emplace_back(std::move(param2));
    columns.emplace_back(std::move(param3));
    int res[] = {0, 0, 0, 0, 0};
    ctx->set_constant_columns(columns);
    ASSERT_TRUE(StringFunctions::field_prepare<TYPE_INT>(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = StringFunctions::field<TYPE_INT>(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::field_close<TYPE_INT>(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());

    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<Int32Column>(result);
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(res[i], v->immutable_data()[i]);
    }
}

PARALLEL_TEST(VecFieldFunctionsTest, fieldIntTest3) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto paramBuilder1 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder2 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder3 = ColumnBuilder<TYPE_INT>(5);

    for (int i = 0; i < 5; i++) {
        paramBuilder1.append(2);
        paramBuilder2.append(1);
        paramBuilder3.append(3);
    }

    auto param1 = paramBuilder1.build(true);
    auto param2 = paramBuilder2.build(true);
    auto param3 = paramBuilder3.build(true);

    columns.emplace_back(std::move(param1));
    columns.emplace_back(std::move(param2));
    columns.emplace_back(std::move(param3));
    int res[] = {0, 0, 0, 0, 0};
    ctx->set_constant_columns(columns);
    ASSERT_TRUE(StringFunctions::field_prepare<TYPE_INT>(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = StringFunctions::field<TYPE_INT>(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::field_close<TYPE_INT>(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(result->is_constant());

    auto v = ColumnHelper::as_column<ConstColumn>(result);
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(res[i], ColumnHelper::as_column<Int32Column>(v->data_column())->immutable_data()[0]);
    }
}

PARALLEL_TEST(VecFieldFunctionsTest, fieldIntTest4) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto paramBuilder1 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder2 = ColumnBuilder<TYPE_INT>(5);
    auto paramBuilder3 = ColumnBuilder<TYPE_INT>(5);

    paramBuilder1.append(3);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append(2);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append(2);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append(2);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    paramBuilder1.append(2);
    paramBuilder2.append(1);
    paramBuilder3.append(3);

    auto param1 = paramBuilder1.build(false);
    auto param2 = paramBuilder2.build(true);
    auto param3 = paramBuilder3.build(true);

    columns.emplace_back(std::move(param1));
    columns.emplace_back(std::move(param2));
    columns.emplace_back(std::move(param3));
    int res[] = {2, 0, 0, 0, 0};
    ctx->set_constant_columns(columns);
    ASSERT_TRUE(StringFunctions::field_prepare<TYPE_INT>(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    ColumnPtr result = StringFunctions::field<TYPE_INT>(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::field_close<TYPE_INT>(ctx.get(), FunctionContext::THREAD_LOCAL).ok());

    ASSERT_TRUE(result->is_numeric());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<Int32Column>(result);
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(res[i], v->immutable_data()[i]);
    }
}

} // namespace starrocks
