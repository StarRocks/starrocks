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

#include <random>

#include "exprs/mock_vectorized_expr.h"
#include "exprs/string_functions.h"

namespace starrocks {

using TestCaseType = std::tuple<std::string, std::string, std::string, bool>;
using TestCases = std::vector<TestCaseType>;
struct UrlExtractParameterTest : public ::testing::Test {
    TestCases test_cases{
            {"https://starrocks.com/doc?k1=10", "k1", "10", false},
            {"https://starrocks.com/doc?kk12=10", "k1", "", true},
            {"https://starrocks.com/doc?kk12=10", "kk1", "", true},
            {"https://starrocks.com/doc?kk12=10", "k12", "", true},
            {"https://starrocks.com/doc?k0=100&k1=10", "k1", "10", false},
            {"https://starrocks.com/doc?k0=abc&kk12=10", "k1", "", true},
            {"https://starrocks.com/doc?k0=100&kk12=10", "kk1", "", true},
            {"https://starrocks.com/doc?k0=1000kk12=10", "k12", "", true},
            {"https://starrocks.com/doc?k0=abc&k1=10#section1", "k1", "10", false},
            {"https://starrocks.com/doc?k0=100&k1=10=20", "k1", "10=20", false},
            {"https://starrocks.com/doc?k0=1000&k1", "k1", "", false},
            {"https://starrocks.com/doc?k0=1000&k1&k1=2", "k1", "", false},
            {"https://starrocks.com/doc?k0=1000&k1&=2", "k1", "", false},
            {"https://starrocks.com/doc?k0=1000&k1=100&k1=200", "k1", "100", false},
            {"https://starrocks.com/doc?k0=1000&k1:200=100&k1=200", "k1:200", "100", false},
            {"https://starrocks.com/doc?k0=1000&k1: 200=100&k1=200", "k1: 200", "", true},
            {"", "k1", "", true},
            {"https://starrocks.com/doc", "k1", "", true},
            {"https://starrocks.com/doc?k0=10&k1=%21%23%24%26%27%28%29%2A%2B%2C%2F%3A"
             "%3B%3D%3F%40%5B%5D%20%22%25%2D%2E%3C%3E%5C%5E%5F%60%7B%7C%7D%7E&k2",
             "k1", "!#$&'()*+,/:;=?@[] \"%-.<>\\^_`{|}~", false},
    };
};

void test_url_extract_parameter(const Columns& columns, const std::string& expect, bool only_null,
                                std::function<bool(int)> row_is_null) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ctx->set_constant_columns(columns);
    StringFunctions::url_extract_parameter_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    auto result = StringFunctions::url_extract_parameter(ctx.get(), columns);
    StringFunctions::url_extract_parameter_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(result.ok());
    auto result_col = result.value();
    auto num_rows = columns[0]->size();
    ASSERT_EQ(result_col->size(), num_rows);
    if (only_null) {
        ASSERT_EQ(ColumnHelper::count_nulls(result_col), num_rows);
    } else {
        for (int i = 0; i < num_rows; ++i) {
            if (row_is_null(i)) {
                ASSERT_TRUE(result_col->is_null(i));
            } else {
                ASSERT_FALSE(result_col->is_null(i));
                auto actual = result_col->get(i).get<Slice>().to_string();
                ASSERT_EQ(actual, expect);
            }
        }
    }
}

TEST_F(UrlExtractParameterTest, constNull) {
    TypeDescriptor varchar_type = TypeDescriptor::create_varchar_type(10000);
    test_url_extract_parameter({ColumnHelper::create_const_null_column(10), ColumnHelper::create_const_null_column(10)},
                               "", true, [](int i) { return false; });
    test_url_extract_parameter(
            {ColumnHelper::unfold_const_column(varchar_type, 10, ColumnHelper::create_const_null_column(10)),
             ColumnHelper::unfold_const_column(varchar_type, 10, ColumnHelper::create_const_null_column(10))},
            "", true, [](int i) { return false; });
    for (TestCaseType tc : test_cases) {
        auto url = std::get<0>(tc);
        auto param_key = std::get<1>(tc);
        auto expect = std::get<2>(tc);

        auto url_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(url, 10);
        auto param_key_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(url, 10);
        test_url_extract_parameter({ColumnHelper::create_const_null_column(10), param_key_col->clone()}, "", true,
                                   [](int i) { return false; });
        test_url_extract_parameter({url_col->clone(), ColumnHelper::create_const_null_column(10)}, "", true,
                                   [](int i) { return false; });
        test_url_extract_parameter({ColumnHelper::create_const_null_column(10),
                                    ColumnHelper::unfold_const_column(varchar_type, 10, param_key_col->clone())},
                                   "", true, [](int i) { return false; });
        test_url_extract_parameter({ColumnHelper::unfold_const_column(varchar_type, 10, url_col->clone()),
                                    ColumnHelper::create_const_null_column(10)},
                                   "", true, [](int i) { return false; });
    }
}

TEST_F(UrlExtractParameterTest, constUrlAndParamKey) {
    for (TestCaseType tc : test_cases) {
        auto url = std::get<0>(tc);
        auto param_key = std::get<1>(tc);
        auto expect = std::get<2>(tc);
        auto only_null = std::get<3>(tc);

        auto url_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(url, 10);
        auto param_key_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(param_key, 10);
        Columns columns{url_col->clone(), param_key_col->clone()};
        test_url_extract_parameter(columns, expect, only_null, [](int i) { return false; });
    }
}

TEST_F(UrlExtractParameterTest, constUrl) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    for (TestCaseType tc : test_cases) {
        auto url = std::get<0>(tc);
        auto param_key = std::get<1>(tc);
        auto expect = std::get<2>(tc);
        auto only_null = std::get<3>(tc);

        auto url_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(url, 10);
        auto param_key_col = BinaryColumn::create();
        auto null_col = NullColumn::create();
        param_key_col->reserve(10);
        null_col->reserve(10);
        auto row_is_null = [](int i) { return i % 2 == 0; };
        for (int i = 0; i < 10; ++i) {
            param_key_col->append(param_key);
            null_col->append(row_is_null(i) ? DATUM_NULL : DATUM_NOT_NULL);
        }
        test_url_extract_parameter({url_col->clone(), param_key_col->clone()}, expect, only_null,
                                   [](int i) { return false; });
        test_url_extract_parameter(
                {url_col->clone(), NullableColumn::create(std::move(param_key_col), std::move(null_col))}, expect,
                only_null, row_is_null);
    }
}

TEST_F(UrlExtractParameterTest, constParamKey) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    for (TestCaseType tc : test_cases) {
        auto url = std::get<0>(tc);
        auto param_key = std::get<1>(tc);
        auto expect = std::get<2>(tc);
        auto only_null = std::get<3>(tc);

        auto url_col = BinaryColumn::create();
        auto null_col = NullColumn::create();
        auto param_key_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(param_key, 10);
        url_col->reserve(10);
        null_col->reserve(10);
        auto row_is_null = [](int i) { return i % 2 == 0; };
        for (int i = 0; i < 10; ++i) {
            url_col->append(url);
            null_col->append(row_is_null(i) ? DATUM_NULL : DATUM_NOT_NULL);
        }
        test_url_extract_parameter({url_col->clone(), param_key_col->clone()}, expect, only_null,
                                   [](int i) { return false; });
        test_url_extract_parameter(
                {NullableColumn::create(url_col->clone(), std::move(null_col)), param_key_col->clone()}, expect,
                only_null, row_is_null);
    }
}

TEST_F(UrlExtractParameterTest, nonConstUrlAndParamKey) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    for (TestCaseType tc : test_cases) {
        auto url = std::get<0>(tc);
        auto param_key = std::get<1>(tc);
        auto expect = std::get<2>(tc);
        auto only_null = std::get<3>(tc);

        auto url_col = BinaryColumn::create();
        auto param_key_col = BinaryColumn::create();
        auto null_col0 = NullColumn::create();
        auto null_col1 = NullColumn::create();
        url_col->reserve(10);
        param_key_col->reserve(10);
        null_col0->reserve(10);
        null_col1->reserve(10);
        auto row_is_null0 = [](int i) { return i % 2 == 0; };
        auto row_is_null1 = [](int i) { return i % 3 == 0; };
        for (int i = 0; i < 10; ++i) {
            url_col->append(url);
            param_key_col->append(param_key);
            null_col0->append(row_is_null0(i) ? DATUM_NULL : DATUM_NOT_NULL);
            null_col1->append(row_is_null1(i) ? DATUM_NULL : DATUM_NOT_NULL);
        }
        test_url_extract_parameter({url_col->clone(), param_key_col->clone()}, expect, only_null,
                                   [](int i) { return false; });
        test_url_extract_parameter(
                {NullableColumn::create(url_col->clone(), null_col0->clone()), param_key_col->clone()}, expect,
                only_null, row_is_null0);
        test_url_extract_parameter(
                {url_col->clone(), NullableColumn::create(param_key_col->clone(), null_col1->clone())}, expect,
                only_null, row_is_null1);
        test_url_extract_parameter({NullableColumn::create(url_col->clone(), null_col0->clone()),
                                    NullableColumn::create(param_key_col->clone(), null_col1->clone())},
                                   expect, only_null, [=](int i) { return row_is_null0(i) || row_is_null1(i); });
    }
}

} // namespace starrocks
