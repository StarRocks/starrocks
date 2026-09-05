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

class StringFunctionBlake3Test : public ::testing::Test {};

TEST_F(StringFunctionBlake3Test, abcA1Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str = BinaryColumn::create();
    str->append("abc");
    columns.emplace_back(std::move(str));

    ColumnPtr result = StringFunctions::blake3(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    std::string s = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";
    ASSERT_EQ(s, v->get_slice(0).to_string());
}

TEST_F(StringFunctionBlake3Test, abcA2Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str = BinaryColumn::create();
    str->append("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    columns.emplace_back(std::move(str));

    ColumnPtr result = StringFunctions::blake3(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    // Known BLAKE3 hash of the SHA-256 test vector long string
    std::string s = "c19012cc2aaf0dc3d8e5c45a1b79114d2df42abb2a410bf54be09e891af06ff8";
    ASSERT_EQ(s, v->get_slice(0).to_string());
}

TEST_F(StringFunctionBlake3Test, abcConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str = BinaryColumn::create();
    str->append("abc");
    columns.emplace_back(ConstColumn::create(std::move(str), 1));
    ColumnPtr result = StringFunctions::blake3(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<ConstColumn>(result)->data_column());

    std::string s = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";
    ASSERT_EQ(s, v->get_slice(0).to_string());
}

TEST_F(StringFunctionBlake3Test, abcNull1Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str1 = BinaryColumn::create();
    auto null = NullColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("abc");
        null->append(j % 2 == 0);
    }
    columns.emplace_back(NullableColumn::create(std::move(str1), std::move(null)));
    ColumnPtr result = StringFunctions::blake3(ctx.get(), columns).value();
    auto nullable_column = ColumnHelper::as_raw_column<NullableColumn>(result);
    auto data_column = ColumnHelper::cast_to<TYPE_VARCHAR>(nullable_column->data_column());

    for (int j = 0; j < 20; ++j) {
        if (j % 2 != 0) {
            std::string s = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";
            ASSERT_EQ(s, data_column->get_slice(j).to_string());
        } else {
            ASSERT_TRUE(nullable_column->is_null(j));
        }
    }
}

TEST_F(StringFunctionBlake3Test, abcNullLiteralTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto str1 = BinaryColumn::create();
    auto null = NullColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("");
        null->append(j % 2 == 0);
    }
    columns.emplace_back(NullableColumn::create(std::move(str1), std::move(null)));
    ColumnPtr result = StringFunctions::blake3(ctx.get(), columns).value();
    auto nullable_column = ColumnHelper::as_raw_column<NullableColumn>(result);
    auto data_column = ColumnHelper::cast_to<TYPE_VARCHAR>(nullable_column->data_column());

    for (int j = 0; j < 20; ++j) {
        if (j % 2 != 0) {
            // BLAKE3 digest of the empty (zero-length) message.
            // https://raw.githubusercontent.com/BLAKE3-team/BLAKE3/master/test_vectors/test_vectors.json
            std::string s = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";
            ASSERT_EQ(s, data_column->get_slice(j).to_string());
        } else {
            ASSERT_TRUE(nullable_column->is_null(j));
        }
    }
}

} // namespace starrocks
