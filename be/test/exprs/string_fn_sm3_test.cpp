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

#include <gtest/gtest.h>

#include "exprs/string_functions.h"

namespace starrocks {

class StringFunctionSm3Test : public ::testing::Test {};
TEST_F(StringFunctionSm3Test, abcA1Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str = BinaryColumn::create();
    str->append("abc");
    columns.emplace_back(std::move(str));

    ColumnPtr result = StringFunctions::sm3(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    std::string s = "66c7f0f4 62eeedd9 d1f2d46b dc10e4e2 4167c487 5cf2f7a2 297da02b 8f4ba8e0";
    ASSERT_EQ(s, v->get_data()[0].to_string());
}

TEST_F(StringFunctionSm3Test, abcA2Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str = BinaryColumn::create();
    str->append("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
    columns.emplace_back(std::move(str));

    ColumnPtr result = StringFunctions::sm3(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    std::string s = "debe9ff9 2275b8a1 38604889 c18e5a4d 6fdb70e5 387e5765 293dcba3 9c0c5732";
    ASSERT_EQ(s, v->get_data()[0].to_string());
}

TEST_F(StringFunctionSm3Test, abcConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str = BinaryColumn::create();
    str->append("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
    columns.emplace_back(ConstColumn::create(std::move(str), 1));
    ColumnPtr result = StringFunctions::sm3(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<ConstColumn>(result)->data_column());

    std::string s = "debe9ff9 2275b8a1 38604889 c18e5a4d 6fdb70e5 387e5765 293dcba3 9c0c5732";
    ASSERT_EQ(s, v->get_data()[0].to_string());
}

TEST_F(StringFunctionSm3Test, abcNull1Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str1 = BinaryColumn::create();
    auto null = NullColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("abc");
        null->append(j % 2 == 0);
    }
    columns.emplace_back(NullableColumn::create(std::move(str1), std::move(null)));
    ColumnPtr result = StringFunctions::sm3(ctx.get(), columns).value();
    auto nullable_column = ColumnHelper::as_raw_column<NullableColumn>(result);
    auto data_column = ColumnHelper::cast_to<TYPE_VARCHAR>(nullable_column->data_column());

    for (int j = 0; j < 20; ++j) {
        if (j % 2 != 0) {
            std::string s = "66c7f0f4 62eeedd9 d1f2d46b dc10e4e2 4167c487 5cf2f7a2 297da02b 8f4ba8e0";
            ASSERT_EQ(s, data_column->get_data()[0].to_string());
        } else {
            ASSERT_TRUE(nullable_column->is_null(j));
        }
    }
}

TEST_F(StringFunctionSm3Test, abcNull2Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str1 = BinaryColumn::create();
    auto null = NullColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        null->append(j % 2 == 0);
    }
    columns.emplace_back(NullableColumn::create(std::move(str1), std::move(null)));
    ColumnPtr result = StringFunctions::sm3(ctx.get(), columns).value();
    auto nullable_column = ColumnHelper::as_raw_column<NullableColumn>(result);
    auto data_column = ColumnHelper::cast_to<TYPE_VARCHAR>(nullable_column->data_column());

    for (int j = 0; j < 20; ++j) {
        if (j % 2 != 0) {
            std::string s = "debe9ff9 2275b8a1 38604889 c18e5a4d 6fdb70e5 387e5765 293dcba3 9c0c5732";
            ASSERT_EQ(s, data_column->get_data()[0].to_string());
        } else {
            ASSERT_TRUE(nullable_column->is_null(j));
        }
    }
}

TEST_F(StringFunctionSm3Test, abcNullLiteralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str1 = BinaryColumn::create();
    auto null = NullColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("");
        null->append(j % 2 == 0);
    }
    columns.emplace_back(NullableColumn::create(std::move(str1), std::move(null)));
    ColumnPtr result = StringFunctions::sm3(ctx.get(), columns).value();
    auto nullable_column = ColumnHelper::as_raw_column<NullableColumn>(result);
    auto data_column = ColumnHelper::cast_to<TYPE_VARCHAR>(nullable_column->data_column());

    for (int j = 0; j < 20; ++j) {
        if (j % 2 != 0) {
            std::string s = "";
            ASSERT_EQ(s, data_column->get_data()[0].to_string());
        } else {
            ASSERT_TRUE(nullable_column->is_null(j));
        }
    }
}

} // namespace starrocks
