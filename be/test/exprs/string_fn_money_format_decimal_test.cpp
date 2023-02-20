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

#include "butil/time.h"
#include "exprs/anyval_util.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/string_functions.h"

namespace starrocks {
class MoneyFormatDecimalTest : public ::testing::Test {};
using TestCase = std::tuple<std::string, std::string>;
using TestArray = std::vector<TestCase>;

template <LogicalType Type>
void test_money_format_decimal(TestArray const& test_cases, int precision, int scale) {
    using ColumnType = RunTimeColumnType<Type>;
    using CppType = RunTimeCppType<Type>;
    std::vector<FunctionContext::TypeDesc> arg_types = {
            AnyValUtil::column_type_to_type_desc(TypeDescriptor::create_decimalv3_type(Type, precision, scale))};
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_test_context(std::move(arg_types), FunctionContext::TypeDesc{}));
    Columns columns;
    auto rows_num = test_cases.size();
    auto money_column = ColumnType::create(precision, scale);
    money_column->reserve(rows_num);
    for (int i = 0; i < rows_num; ++i) {
        CppType value;
        auto money = std::get<0>(test_cases[i]);
        DecimalV3Cast::from_string<CppType>(&value, precision, scale, money.c_str(), money.size());
        money_column->append(value);
    }

    columns.emplace_back(money_column);
    ColumnPtr result = StringFunctions::money_format_decimal<Type>(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < rows_num; ++i) {
        auto actual = v->get_data()[i].to_string();
        auto expect = std::get<1>(test_cases[i]);
        std::cout << "decimal=" << std::get<0>(test_cases[i]) << ", actual=" << actual << ", expect=" << expect
                  << std::endl;
        ASSERT_EQ(actual, expect);
    }
}

TEST_F(MoneyFormatDecimalTest, moneyFormatDecimalScaleEqZero) {
    TestArray test_cases = {
            {"0", "0.00"},
            {"9999999", "9,999,999.00"},
            {"-999999", "-999,999.00"},
            {"1", "1.00"},
            {"1234567", "1,234,567.00"},
            {"-1", "-1.00"},
            {"-1234567", "-1,234,567.00"},
    };
    test_money_format_decimal<TYPE_DECIMAL32>(test_cases, 9, 0);
    test_money_format_decimal<TYPE_DECIMAL64>(test_cases, 18, 0);
    test_money_format_decimal<TYPE_DECIMAL128>(test_cases, 38, 0);
}

TEST_F(MoneyFormatDecimalTest, moneyFormatDecimalScaleEqTwo) {
    TestArray test_cases = {
            {"0", "0.00"},
            {"9999999.99", "9,999,999.99"},
            {"-9999999.99", "-9,999,999.99"},
            {"1.01", "1.01"},
            {"12345.67", "12,345.67"},
            {"-1.01", "-1.01"},
            {"-12345.67", "-12,345.67"},
    };
    test_money_format_decimal<TYPE_DECIMAL32>(test_cases, 9, 2);
    test_money_format_decimal<TYPE_DECIMAL64>(test_cases, 18, 2);
    test_money_format_decimal<TYPE_DECIMAL128>(test_cases, 38, 2);
}

TEST_F(MoneyFormatDecimalTest, moneyFormatDecimalScaleEqPrecision) {
    TestArray test_cases = {{"0", "0.00"},         {"0.999999999", "1.00"}, {"-0.99", "-0.99"},
                            {"0.000001", "0.00"},  {"0.1234567", "0.12"},   {"-0.101", "-0.10"},
                            {"-0.55555", "-0.56"}, {"0.555555", "0.56"},    {"-0.01", "-0.01"}};
    test_money_format_decimal<TYPE_DECIMAL32>(test_cases, 9, 9);
    test_money_format_decimal<TYPE_DECIMAL64>(test_cases, 18, 18);
    test_money_format_decimal<TYPE_DECIMAL128>(test_cases, 38, 38);
}

} // namespace starrocks
