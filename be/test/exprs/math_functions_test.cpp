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

#include "exprs/math_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cmath>

#include "exprs/mock_vectorized_expr.h"

#define PI acos(-1)

namespace starrocks {
class VecMathFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(VecMathFunctionsTest, truncateTest) {
    {
        Columns columns;

        auto c0 = DoubleColumn::create();
        auto c1 = Int32Column::create();

        double dous[] = {2341.2341111, 4999.90134, 2144.2855, 934.12439};
        int ints[] = {2, 3, 1, 4};

        double expected_res[] = {2341.23, 4999.901, 2144.2, 934.1243};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            c0->append(dous[i]);
            c1->append(ints[i]);
        }

        columns.emplace_back(c0);
        columns.emplace_back(c1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr res = MathFunctions::truncate(ctx.get(), columns).value();

        auto* raw_res = ColumnHelper::cast_to<TYPE_DOUBLE>(res)->get_data().data();

        for (int i = 0; i < sizeof(expected_res) / sizeof(expected_res[0]); ++i) {
            ASSERT_EQ(expected_res[i], raw_res[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, truncateNanTest) {
    {
        Columns columns;

        auto c0 = DoubleColumn::create();
        auto c1 = Int32Column::create();

        c0->append(0);
        c1->append(1591994755);

        columns.emplace_back(c0);
        columns.emplace_back(c1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr res = MathFunctions::truncate(ctx.get(), columns).value();

        ASSERT_EQ(true, res->is_null(0));
    }
}

static constexpr uint8_t TYPE_ROUND = 0;
static constexpr uint8_t TYPE_ROUND_UP_TO = 1;
static constexpr uint8_t TYPE_TRUNCATE = 2;

template <uint8_t type>
static void testRoundDecimal(const std::vector<std::string>& arg0_values, const std::vector<uint8_t>& arg0_null_flags,
                             const int32_t precision, const int32_t scale, const std::vector<int32_t>& arg1_values,
                             const std::vector<uint8_t>& arg1_null_flags, const std::vector<std::string>& res_values,
                             const std::vector<uint8_t>& res_null_flags) {
    ASSERT_GE(precision, 0);
    ASSERT_LE(precision, decimal_precision_limit<int128_t>);
    ASSERT_GE(scale, 0);
    ASSERT_LE(scale, decimal_precision_limit<int128_t>);
    ASSERT_TRUE(arg0_null_flags.empty() || arg0_null_flags.size() == arg0_values.size());
    ASSERT_TRUE(arg1_null_flags.empty() || arg1_null_flags.size() == arg1_values.size());
    ASSERT_TRUE(res_null_flags.empty() || res_null_flags.size() == res_values.size());

    Columns columns;

    auto arg0_data_column = DecimalV3Column<int128_t>::create(precision, scale);
    auto arg1_data_column = Int32Column::create();

    ASSERT_EQ(arg0_values.size(), arg1_values.size());
    std::set<std::string> arg0_distinct_values;
    bool arg0_has_null = false;
    bool arg0_all_null = true;
    for (auto i = 0; i < arg0_values.size(); i++) {
        if (!arg0_null_flags.empty() && arg0_null_flags[i] == 1) {
            arg0_has_null = true;
            arg0_data_column->append_default(1);
            continue;
        }
        arg0_all_null = false;
        int128_t value;
        DecimalV3Cast::from_string<int128_t>(&value, precision, scale, arg0_values[i].c_str(), arg0_values[i].length());
        arg0_data_column->append(value);
        arg0_distinct_values.insert(arg0_values[i]);
    }
    std::set<int32_t> arg1_distinct_values;
    bool arg1_has_null = false;
    bool arg1_all_null = true;
    for (auto i = 0; i < arg1_values.size(); i++) {
        if (!arg1_null_flags.empty() && arg1_null_flags[i] == 1) {
            arg1_has_null = true;
            arg1_data_column->append_default(1);
            continue;
        }
        arg1_all_null = false;
        arg1_data_column->append(arg1_values[i]);
        arg1_distinct_values.insert(arg1_values[i]);
    }

    ColumnPtr c0;
    bool c0_const = false;
    bool c0_nullable = false;
    if (arg0_all_null) {
        // ConstNullColumn
        c0_const = true;
        c0_nullable = true;
        c0 = ColumnHelper::create_const_null_column(arg0_data_column->size());
    } else if (!arg0_has_null && arg0_distinct_values.size() == 1) {
        // ConstColumn
        c0_const = true;
        arg0_data_column->resize(1);
        c0 = ConstColumn::create(arg0_data_column, arg0_values.size());
    } else {
        if (arg0_null_flags.empty()) {
            // normal Column
            c0 = arg0_data_column;
        } else {
            // NullableColumn
            c0_nullable = true;
            auto null_flags = NullColumn::create();
            for (int i = 0; i < arg0_values.size(); i++) {
                null_flags->append(arg0_null_flags[i]);
            }
            c0 = NullableColumn::create(arg0_data_column, null_flags);
        }
    }

    ColumnPtr c1;
    bool c1_const = false;
    bool c1_nullable = false;
    if (arg1_all_null) {
        // ConstNullColumn
        c1_const = true;
        c1_nullable = true;
        c1 = ColumnHelper::create_const_null_column(arg1_data_column->size());
    } else if (!arg1_has_null && arg1_distinct_values.size() == 1) {
        // ConstColumn
        c1_const = true;
        arg1_data_column->resize(1);
        c1 = ConstColumn::create(arg1_data_column, arg1_values.size());
    } else {
        if (arg1_null_flags.empty()) {
            // normal Column
            c1 = arg1_data_column;
        } else {
            // NullableColumn
            c1_nullable = true;
            auto null_flags = NullColumn::create();
            for (int i = 0; i < arg1_values.size(); i++) {
                null_flags->append(arg1_null_flags[i]);
            }
            c1 = NullableColumn::create(arg1_data_column, null_flags);
        }
    }

    columns.emplace_back(c0);
    if (type == TYPE_ROUND) {
        c1_const = true;
        c1_nullable = false;
    } else {
        columns.emplace_back(c1);
    }

    FunctionContext::TypeDesc return_type;
    return_type.precision = decimal_precision_limit<int128_t>;
    if (arg1_values.size() == 1 || c1_const) {
        if (arg1_values[0] < 0) {
            return_type.scale = 0;
        } else if (arg1_values[0] > decimal_precision_limit<int128_t>) {
            return_type.scale = decimal_precision_limit<int128_t>;
        } else {
            return_type.scale = arg1_values[0];
        }
    } else {
        return_type.scale = scale;
    }
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_test_context(std::vector<FunctionContext::TypeDesc>(), return_type));
    ColumnPtr res_column;
    bool res_const = false;
    if (type == TYPE_ROUND) {
        res_column = MathFunctions::round_decimal128(ctx.get(), columns).value();
    } else if (type == TYPE_ROUND_UP_TO) {
        res_column = MathFunctions::round_up_to_decimal128(ctx.get(), columns).value();
    } else {
        ASSERT_EQ(type, TYPE_TRUNCATE);
        res_column = MathFunctions::truncate_decimal128(ctx.get(), columns).value();
    }
    DecimalV3Column<int128_t>* decimal_res_column;
    NullColumn* null_fags_res_column = nullptr;

    if ((c0_const && c0_nullable) || (c1_const && c1_nullable)) {
        ASSERT_TRUE(res_column->only_null());
        res_const = true;
        return;
    } else if (c0_const && c1_const) {
        ASSERT_TRUE(res_column->is_constant());
        res_const = true;
        auto maybe_nullable_res_column = FunctionHelper::get_data_column_of_const(res_column);
        if (maybe_nullable_res_column->is_nullable()) {
            decimal_res_column = nullptr;
            null_fags_res_column = down_cast<NullableColumn*>(maybe_nullable_res_column.get())->null_column().get();
        } else {
            decimal_res_column = ColumnHelper::cast_to<TYPE_DECIMAL128>(maybe_nullable_res_column).get();
        }
    } else if (c0_nullable || c1_nullable) {
        ASSERT_TRUE(res_column->is_nullable());
        decimal_res_column =
                ColumnHelper::cast_to<TYPE_DECIMAL128>(FunctionHelper::get_data_column_of_nullable(res_column)).get();
        null_fags_res_column = down_cast<NullableColumn*>(res_column.get())->null_column().get();
    } else {
        ASSERT_FALSE(res_column->is_constant());
        auto maybe_nullable_res_column = FunctionHelper::get_data_column_of_const(res_column);
        if (maybe_nullable_res_column->is_nullable()) {
            decimal_res_column = ColumnHelper::cast_to<TYPE_DECIMAL128>(
                                         FunctionHelper::get_data_column_of_nullable(maybe_nullable_res_column))
                                         .get();
            null_fags_res_column = down_cast<NullableColumn*>(maybe_nullable_res_column.get())->null_column().get();
        } else {
            decimal_res_column = ColumnHelper::cast_to<TYPE_DECIMAL128>(maybe_nullable_res_column).get();
        }
    }

    if (decimal_res_column != nullptr) {
        ASSERT_EQ(decimal_precision_limit<int128_t>, decimal_res_column->precision());
        ASSERT_EQ(return_type.scale, decimal_res_column->scale());
    }
    if (res_null_flags.size() == res_values.size()) {
        ASSERT_TRUE(null_fags_res_column != nullptr);
        const auto end = res_const ? 0 : res_values.size();
        for (auto i = 0; i < end; i++) {
            if (res_null_flags[i] > 0) {
                ASSERT_EQ(res_null_flags[i], null_fags_res_column->get_data()[i]);
            } else {
                ASSERT_EQ(res_values[i], DecimalV3Cast::to_string<int128_t>(decimal_res_column->get_data()[i],
                                                                            return_type.precision, return_type.scale));
            }
        }
    } else {
        const auto end = res_const ? 0 : res_values.size();
        for (auto i = 0; i < end; i++) {
            ASSERT_EQ(res_values[i], DecimalV3Cast::to_string<int128_t>(decimal_res_column->get_data()[i],
                                                                        return_type.precision, return_type.scale));
        }
    }
}

TEST_F(VecMathFunctionsTest, DecimalRoundTest) {
    testRoundDecimal<TYPE_ROUND>({"18450.76"}, {}, 10, 2, {0}, {}, {"18451"}, {});
    testRoundDecimal<TYPE_ROUND>({"0.1"}, {}, 10, 1, {0}, {}, {"0"}, {});
    testRoundDecimal<TYPE_ROUND>({"0.499"}, {}, 10, 3, {0}, {}, {"0"}, {});
    testRoundDecimal<TYPE_ROUND>({"0.5"}, {}, 10, 1, {0}, {}, {"1"}, {});
    testRoundDecimal<TYPE_ROUND>({"0.50"}, {}, 10, 2, {0}, {}, {"1"}, {});
    testRoundDecimal<TYPE_ROUND>({"-0.1"}, {}, 10, 1, {0}, {}, {"0"}, {});
    testRoundDecimal<TYPE_ROUND>({"-0.5"}, {}, 10, 1, {0}, {}, {"-1"}, {});
    testRoundDecimal<TYPE_ROUND>({"-0.50"}, {}, 10, 2, {0}, {}, {"-1"}, {});
    testRoundDecimal<TYPE_ROUND>({"-0.91"}, {}, 10, 2, {0}, {}, {"-1"}, {});
}

TEST_F(VecMathFunctionsTest, DecimalRoundNullTest) {
    testRoundDecimal<TYPE_ROUND>({"INVALID"}, {1}, 10, 2, {0}, {}, {"INVALID"}, {1});

    testRoundDecimal<TYPE_ROUND>({"INVALID", "INVALID"}, {1, 1}, 10, 2, {0, 0}, {}, {"INVALID", "INVALID"}, {1, 1});
}

TEST_F(VecMathFunctionsTest, DecimalRoundUpToTest) {
    testRoundDecimal<TYPE_ROUND_UP_TO>({"18450.76"}, {}, 10, 2, {-1}, {}, {"18450"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"18450.76"}, {}, 10, 2, {0}, {}, {"18451"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"18450.76"}, {}, 10, 2, {1}, {}, {"18450.8"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"18450.76"}, {}, 10, 2, {2}, {}, {"18450.76"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"18450.76"}, {}, 10, 2, {5}, {}, {"18450.76000"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"0.1"}, {}, 10, 2, {38}, {}, {"0.10000000000000000000000000000000000000"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"0.1"}, {}, 10, 2, {1000}, {}, {"0.10000000000000000000000000000000000000"},
                                       {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"13.14"}, {}, 10, 2, {-1}, {}, {"10"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"13.14"}, {}, 10, 2, {-2}, {}, {"0"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"13.14"}, {}, 10, 2, {-100}, {}, {"0"}, {});

    testRoundDecimal<TYPE_ROUND_UP_TO>({"12345.67899", "12345.67899", "12345.67899", "12345.67899", "12345.67899"}, {},
                                       15, 5, {2, 2, 2, 2, 2}, {},
                                       {"12345.68", "12345.68", "12345.68", "12345.68", "12345.68"}, {});
}

TEST_F(VecMathFunctionsTest, DecimalRoundUpToNullTest) {
    testRoundDecimal<TYPE_ROUND_UP_TO>({"18450.76"}, {}, 10, 2, {0}, {1}, {"INVALID"}, {1});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"INVALID"}, {1}, 10, 2, {2}, {}, {"INVALID"}, {1});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"INVALID"}, {1}, 10, 2, {0}, {1}, {"INVALID"}, {1});

    testRoundDecimal<TYPE_ROUND_UP_TO>({"18450.76", "18450.76"}, {}, 10, 2, {0, 0}, {1, 1}, {"INVALID", "INVALID"},
                                       {1, 1});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"INVALID", "INVALID"}, {1, 1}, 10, 2, {2, 2}, {}, {"INVALID", "INVALID"},
                                       {1, 1});
    testRoundDecimal<TYPE_ROUND_UP_TO>({"INVALID", "INVALID"}, {1, 1}, 10, 2, {0, 0}, {1, 1}, {"INVALID", "INVALID"},
                                       {1, 1});

    testRoundDecimal<TYPE_ROUND_UP_TO>({"123456789.11"}, {}, 15, 2, {38}, {}, {"INVALID"}, {1});
}

TEST_F(VecMathFunctionsTest, DecimalRoundUpToByColTest) {
    testRoundDecimal<TYPE_ROUND_UP_TO>(
            {"123.45678", "123.45678", "123.45678", "123.45678", "123.45678", "123.45678", "123.45678"}, {}, 15, 5,
            {0, 1, 2, 3, 4, 5, 6}, {},
            {"123.00000", "123.50000", "123.46000", "123.45700", "123.45680", "123.45678", "123.45678"}, {});
    testRoundDecimal<TYPE_ROUND_UP_TO>(
            {"123.45678", "INVALID", "123.45678", "123.45678", "123.45678", "123.45678", "123.45678"},
            {0, 1, 0, 0, 0, 0, 0}, 15, 5, {0, 1, 2, 3, 4, 5, 6}, {0, 0, 0, 0, 0, 0, 1},
            {"123.00000", "INVALID", "123.46000", "123.45700", "123.45680", "123.45678", "INVALID"},
            {0, 1, 0, 0, 0, 0, 1});
    testRoundDecimal<TYPE_ROUND_UP_TO>(
            {"13.14", "13.14", "13.14", "13.14", "13.14", "13.14", "13.14", "13.14", "13.14"}, {}, 10, 2,
            {-100, -3, -2, -1, 0, 1, 2, 3, 100}, {},
            {"INVALID", "0.00", "0.00", "10.00", "13.00", "13.10", "13.14", "13.14", "13.14"},
            {1, 0, 0, 0, 0, 0, 0, 0, 0});
}

TEST_F(VecMathFunctionsTest, DecimalTruncateTest) {
    testRoundDecimal<TYPE_TRUNCATE>({"18450.76"}, {}, 10, 2, {-1}, {}, {"18450"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"18450.76"}, {}, 10, 2, {0}, {}, {"18450"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"18450.76"}, {}, 10, 2, {1}, {}, {"18450.7"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"18450.76"}, {}, 10, 2, {2}, {}, {"18450.76"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"18450.76"}, {}, 10, 2, {5}, {}, {"18450.76000"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"0.1"}, {}, 10, 2, {38}, {}, {"0.10000000000000000000000000000000000000"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"0.1"}, {}, 10, 2, {1000}, {}, {"0.10000000000000000000000000000000000000"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"13.14"}, {}, 10, 2, {-1}, {}, {"10"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"13.14"}, {}, 10, 2, {-2}, {}, {"0"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"13.14"}, {}, 10, 2, {-100}, {}, {"0"}, {});

    testRoundDecimal<TYPE_TRUNCATE>({"12345.67899", "12345.67899", "12345.67899", "12345.67899", "12345.67899"}, {}, 15,
                                    5, {2, 2, 2, 2, 2}, {},
                                    {"12345.67", "12345.67", "12345.67", "12345.67", "12345.67"}, {});
}

TEST_F(VecMathFunctionsTest, DecimalTruncateNullTest) {
    testRoundDecimal<TYPE_TRUNCATE>({"18450.76"}, {}, 10, 2, {0}, {1}, {"INVALID"}, {1});
    testRoundDecimal<TYPE_TRUNCATE>({"INVALID"}, {1}, 10, 2, {2}, {}, {"INVALID"}, {1});
    testRoundDecimal<TYPE_TRUNCATE>({"INVALID"}, {1}, 10, 2, {0}, {1}, {"INVALID"}, {1});

    testRoundDecimal<TYPE_TRUNCATE>({"18450.76", "18450.76"}, {}, 10, 2, {0, 0}, {1, 1}, {"INVALID", "INVALID"},
                                    {1, 1});
    testRoundDecimal<TYPE_TRUNCATE>({"INVALID", "INVALID"}, {1, 1}, 10, 2, {2, 2}, {}, {"INVALID", "INVALID"}, {1, 1});
    testRoundDecimal<TYPE_TRUNCATE>({"INVALID", "INVALID"}, {1, 1}, 10, 2, {0, 0}, {1, 1}, {"INVALID", "INVALID"},
                                    {1, 1});

    testRoundDecimal<TYPE_TRUNCATE>({"123456789.11"}, {}, 15, 2, {38}, {}, {"INVALID"}, {1});
}

TEST_F(VecMathFunctionsTest, DecimalTruncateByColTest) {
    testRoundDecimal<TYPE_TRUNCATE>(
            {"123.45678", "123.45678", "123.45678", "123.45678", "123.45678", "123.45678", "123.45678"}, {}, 15, 5,
            {0, 1, 2, 3, 4, 5, 6}, {},
            {"123.00000", "123.40000", "123.45000", "123.45600", "123.45670", "123.45678", "123.45678"}, {});
    testRoundDecimal<TYPE_TRUNCATE>(
            {"123.45678", "INVALID", "123.45678", "123.45678", "123.45678", "123.45678", "123.45678"},
            {0, 1, 0, 0, 0, 0, 0}, 15, 5, {0, 1, 2, 3, 4, 5, 6}, {0, 0, 0, 0, 0, 0, 1},
            {"123.00000", "INVALID", "123.45000", "123.45600", "123.45670", "123.45678", "INVALID"},
            {0, 1, 0, 0, 0, 0, 1});
    // truncate(v,d), v is const column
    testRoundDecimal<TYPE_TRUNCATE>({"12345.6789", "12345.6789", "12345.6789", "12345.6789"}, {}, 15, 5, {1, 2, 3, 4},
                                    {}, {"12345.60000", "12345.67000", "12345.67800", "12345.67890"}, {});
    testRoundDecimal<TYPE_TRUNCATE>({"13.14", "13.14", "13.14", "13.14", "13.14", "13.14", "13.14", "13.14", "13.14"},
                                    {}, 10, 2, {-100, -3, -2, -1, 0, 1, 2, 3, 100}, {},
                                    {"INVALID", "0.00", "0.00", "10.00", "13.00", "13.10", "13.14", "13.14", "13.14"},
                                    {1, 0, 0, 0, 0, 0, 0, 0, 0});
}

TEST_F(VecMathFunctionsTest, RoundUpToTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        double dous[] = {2341.2341111, 4999.90134, 2144.2855, 934.12439};
        int ints[] = {2, 3, 1, 4};

        double res[] = {2341.23, 4999.901, 2144.3, 934.1244};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            tc1->append(dous[i]);
            tc2->append(ints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::round_up_to(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, RoundUpToHalfwayCasesWithPositiveTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        double dous[] = {7.845, 7.855};
        int ints[] = {2, 2};

        double res[] = {7.85, 7.86};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            tc1->append(dous[i]);
            tc2->append(ints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::round_up_to(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, RoundUpToHalfwayCasesWithNegativeTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        double dous[] = {45.0, 44.0};
        int ints[] = {-1, -1};

        double res[] = {50, 40};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            tc1->append(dous[i]);
            tc2->append(ints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::round_up_to(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, BinTest) {
    {
        Columns columns;

        auto tc1 = Int64Column::create();

        int64_t ints[] = {1, 2, 256};

        std::string res[] = {"1", "10", "100000000"};

        for (long i : ints) {
            tc1->append(i);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::bin(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i].to_string());
        }
    }
}

TEST_F(VecMathFunctionsTest, LeastDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.2222222222", "-740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc1->append(dec_value);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"2342.111", "9866.9011"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc2->append(dec_value);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template least<TYPE_DECIMALV2>(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"2342.111", "-740740740.716049"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, GreatestDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.2222222222", "-740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc1->append(dec_value);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"2342.111", "9866.9011"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc2->append(dec_value);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template greatest<TYPE_DECIMALV2>(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"3333333333.2222222222", "9866.9011"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, PositiveDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"-3333333333.2222222222", "-740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc1->append(dec_value);
        }
    }

    columns.emplace_back(tc1);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template positive<TYPE_DECIMALV2>(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"-3333333333.2222222222", "-740740740.716049"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, NegativeDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.2222222222", "740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc1->append(dec_value);
        }
    }

    columns.emplace_back(tc1);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template negative<TYPE_DECIMALV2>(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"-3333333333.2222222222", "-740740740.716049"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, ModDecimalGeneralTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.3222222222", "2342414342.132", "32413241.12342", "999234812.222"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc1->append(dec_value);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"4", "3", "0", "1"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc2->append(dec_value);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template mod<TYPE_DECIMALV2>(ctx.get(), columns).value();

    //auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);
    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    std::string str[] = {"1.3222222222", "2.132", "0.12342", "0.222"};
    DecimalV2Value results[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        results[i] = DecimalV2Value(str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, ModDecimalBigTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {
                "333333099873333.322222222", "23112133142414342.132", "413241.12342", "999234812.222", "68482.48227",
                "2413424287348.24221"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc1->append(dec_value);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"4535.3452", "7.34535", "2.91", "71.234", "34241.24114", "777982341.234234"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (auto dec_value : dec_values) {
            tc2->append(dec_value);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template mod<TYPE_DECIMALV2>(ctx.get(), columns).value();

    //auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);
    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    std::string str[] = {"163.573422222", "4.4786", "0.75342", "19.69", "34241.24113", "123064839.648342"};
    DecimalV2Value results[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        results[i] = DecimalV2Value(str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, Conv_intTest) {
    {
        Columns columns;

        auto tc1 = Int64Column::create();
        auto tc2 = Int8Column::create();
        auto tc3 = Int8Column::create();

        int64_t bigints[] = {1,           2,           35,          10,         10,         10,         10,
                             3,           3,           99999999,    99999999,   99999999,   99999999,   -99999999,
                             -99999999,   -99999999,   -99999999,   4294967296, 4294967296, 4294967296, 4294967296,
                             -4294967296, -4294967296, -4294967296, -4294967296};
        int8_t baseints[] = {10,  10,  10, 2,  2,   -2,  -2, -2, -2,  -16, -16, 16, 16,
                             -16, -16, 16, 16, -16, -16, 16, 16, -16, -16, -16, -16};
        int8_t destints[] = {10, 16,  36, 10,  -10, 10,  -10, 10,  10, 10,  -10, 10, -10,
                             10, -10, 10, -10, 10,  -10, 10,  -10, 10, -10, 10,  -10};

        std::string results[] = {"1",
                                 "2",
                                 "Z",
                                 "2",
                                 "2",
                                 "2",
                                 "2",
                                 "0",
                                 "0",
                                 "2576980377",
                                 "2576980377",
                                 "2576980377",
                                 "2576980377",
                                 "18446744071132571239",
                                 "-2576980377",
                                 "18446744071132571239",
                                 "-2576980377",
                                 "285960729238",
                                 "285960729238",
                                 "285960729238",
                                 "285960729238",
                                 "18446743787748822378",
                                 "-285960729238",
                                 "18446743787748822378",
                                 "-285960729238"};

        for (int i = 0; i < sizeof(bigints) / sizeof(bigints[0]); ++i) {
            tc1->append(bigints[i]);
            tc2->append(baseints[i]);
            tc3->append(destints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);
        columns.emplace_back(tc3);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::conv_int(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

        for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i].to_string());
        }
    }
}

TEST_F(VecMathFunctionsTest, Conv_stringTest) {
    {
        Columns columns;

        auto tc1 = BinaryColumn::create();
        auto tc2 = Int8Column::create();
        auto tc3 = Int8Column::create();

        std::string bigints[] = {"1",
                                 "2",
                                 "35",
                                 "1000",
                                 "103",
                                 "114",
                                 "18446744073709551617",
                                 "10",
                                 "10",
                                 "10",
                                 "10",
                                 "-9223372036854775808",
                                 "-9223372036854775808",
                                 "-9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775808",
                                 "9223372036854775808",
                                 "9223372036854775808",
                                 "9223372036854775808",
                                 "-9223372036854775809",
                                 "-9223372036854775809",
                                 "-9223372036854775809",
                                 "-9223372036854775809",
                                 "18446744073709551616",
                                 "18446744073709551616",
                                 "18446744073709551616",
                                 "18446744073709551616",
                                 "-18446744073709551616",
                                 "-18446744073709551616",
                                 "-18446744073709551616",
                                 "-18446744073709551616",
                                 "de0b6b3a7640000",
                                 "8ac7230489e80000",
                                 "a"};
        int8_t baseints[] = {10, 10, 10,  8,   35, 35, 10,  -2,  -2, 2,  2,   -10, -10, 10, 10, -10, -10,
                             10, 10, -10, -10, 10, 10, -10, -10, 10, 10, -10, -10, 10,  10, 16, 16,  10};
        int8_t destints[] = {10, 16,  36, 10,  10, 10,  10, 10,  -10, 10,  -10, 10,  -10, 10,  -10, 10, -10,
                             10, -10, 10, -10, 10, -10, 10, -10, 10,  -10, 10,  -10, 10,  -10, 10,  10, 10};

        std::string results[] = {"1",
                                 "2",
                                 "Z",
                                 "512",
                                 "1228",
                                 "1264",
                                 "18446744073709551615",
                                 "2",
                                 "2",
                                 "2",
                                 "2",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "18446744073709551615",
                                 "-1",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "0",
                                 "0",
                                 "1000000000000000000",
                                 "10000000000000000000",
                                 "0"};

        for (int i = 0; i < sizeof(bigints) / sizeof(bigints[0]); ++i) {
            tc1->append(bigints[i]);
            tc2->append(baseints[i]);
            tc3->append(destints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);
        columns.emplace_back(tc3);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::conv_string(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

        for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i].to_string());
        }
    }
}

TEST_F(VecMathFunctionsTest, LnTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        tc1->append(0);
        tc1->append(2.0);
        tc1->append(-1);
        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result_log10 = MathFunctions::log10(ctx.get(), columns).value();
        ColumnPtr result_ln = MathFunctions::ln(ctx.get(), columns).value();

        ASSERT_EQ(true, result_log10->is_null(0));
        ASSERT_EQ(std::log10(2), result_log10->get(1).get_double());
        ASSERT_EQ(true, result_log10->is_null(2));

        ASSERT_EQ(true, result_ln->is_null(0));
        ASSERT_EQ(std::log(2), result_ln->get(1).get_double());
        ASSERT_EQ(true, result_ln->is_null(2));
    }
}

TEST_F(VecMathFunctionsTest, ExpTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        tc1->append(0);
        tc1->append(2.0);
        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result_exp = MathFunctions::exp(ctx.get(), columns).value();

        ASSERT_EQ(false, result_exp->is_null(0));
        ASSERT_EQ(std::exp(0), result_exp->get(0).get_double());
        ASSERT_EQ(std::exp(2), result_exp->get(1).get_double());
    }
}

TEST_F(VecMathFunctionsTest, ExpOverflowTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        tc1->append(2.47498282E8);
        tc1->append(2.47498282E3);
        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result_exp = MathFunctions::exp(ctx.get(), columns).value();

        ASSERT_EQ(true, result_exp->is_null(0));
        ASSERT_EQ(true, result_exp->is_null(1));
    }
}

TEST_F(VecMathFunctionsTest, squareTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        tc1->append(0);
        tc1->append(2.0);
        tc1->append(-1);
        tc1->append(std::nan("not a double number"));
        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result_square = MathFunctions::square(ctx.get(), columns).value();

        ASSERT_EQ(0, result_square->get(0).get_double());
        ASSERT_EQ(4, result_square->get(1).get_double());
        ASSERT_EQ(1, result_square->get(2).get_double());
        ASSERT_EQ(true, result_square->is_null(3));
    }
}

TEST_F(VecMathFunctionsTest, AbsTest) {
    {
        Columns columns;

        auto tc1 = Int8Column::create();
        int8_t inputs[] = {10, 10, 10, 8, -35, 35, -128};
        int16_t results[] = {10, 10, 10, 8, 35, 35, 128};

        for (signed char input : inputs) {
            tc1->append(input);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_tinyint(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_SMALLINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int16Column::create();
        int16_t inputs[] = {1000, 1000, 1000, -500, -35, 35, -32768};
        int32_t results[] = {1000, 1000, 1000, 500, 35, 35, 32768};

        for (short input : inputs) {
            tc1->append(input);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_smallint(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int32Column::create();
        int32_t inputs[] = {23424242, -1111188324, 909877, -4353525, -39879711, 35, -2147483648};
        int64_t results[] = {23424242, 1111188324, 909877, 4353525, 39879711, 35, 2147483648};

        for (int input : inputs) {
            tc1->append(input);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_int(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int64Column::create();
        int64_t inputs[] = {2342423422442, -11111883200004,         90987700000, -435352241435, -398797110000,
                            3523411,       -9223372036854775807 - 1};
        int128_t results[] = {2342423422442, 11111883200004, 90987700000,         435352241435,
                              398797110000,  3523411,        9223372036854775808U};

        for (long input : inputs) {
            tc1->append(input);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_bigint(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_LARGEINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int128Column::create();
        int128_t inputs[] = {2342423422442, -1111188320000004,   -90987700000241,
                             -435352241435, -390241238797110000, -3523411};
        int128_t results[] = {2342423422442, 1111188320000004,   90987700000241,
                              435352241435,  390241238797110000, 3523411};

        for (__int128 input : inputs) {
            tc1->append(input);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_largeint(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_LARGEINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        double inputs[] = {23424222.124, -34123412.24311, -23412487.1111, -97241.8761, -2349723, -3523411};
        double results[] = {23424222.124, 34123412.24311, 23412487.1111, 97241.8761, 2349723, 3523411};

        for (double input : inputs) {
            tc1->append(input);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_double(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = FloatColumn::create();
        float inputs[] = {23424222.124, -34123412.24311, -23412487.1111, -97241.8761, -2349723, -3523411};
        float results[] = {23424222.124, 34123412.24311, 23412487.1111, 97241.8761, 2349723, 3523411};

        for (float input : inputs) {
            tc1->append(input);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_float(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_FLOAT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = DecimalColumn::create();
        {
            std::string str[] = {"3333333333.2222222222", "-740740740.716049"};
            DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
            for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
                dec_values[i] = DecimalV2Value(str[i]);
            }
            for (auto dec_value : dec_values) {
                tc1->append(dec_value);
            }
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_decimalv2val(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

        std::string result_str[] = {"3333333333.2222222222", "740740740.716049"};
        DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
        for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
            results[i] = DecimalV2Value(result_str[i]);
        }

        for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, CotTest) {
    {
        Columns columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(0);
        tc1->append(0.1);
        tc1->append(0.2);
        tc1->append(0.3);
        columns.emplace_back(tc1);

        Columns columns2;
        auto tc2 = DoubleColumn::create();
        tc2->append(0);
        tc2->append(0.1);
        tc2->append(0.2);
        tc2->append(0.3);
        columns2.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::cot(ctx.get(), columns).value();
        ColumnPtr result2 = MathFunctions::tan(ctx.get(), columns2).value();

        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(nullable->data_column());
        auto v2 = ColumnHelper::cast_to<TYPE_DOUBLE>(result2);

        ASSERT_TRUE(nullable->is_null(0));
        for (int i = 1; i < 4; ++i) {
            LOG(INFO) << "v->get_data()[i]: " << v->get_data()[i] << " v2->get_data()[i]: " << v2->get_data()[i];
            ASSERT_TRUE((1 - v->get_data()[i] * v2->get_data()[i]) < 0.1);
        }
    }
}

TEST_F(VecMathFunctionsTest, Atan2Test) {
    {
        Columns columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(0.1);
        tc1->append(0.2);
        tc1->append(0.3);
        columns.emplace_back(tc1);

        auto tc2 = DoubleColumn::create();
        tc2->append(0.1);
        tc2->append(0.2);
        tc2->append(0.3);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::atan2(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE((v->get_data()[i] - (PI / 4)) < 0.1);
        }
    }
}

TEST_F(VecMathFunctionsTest, TrigonometricFunctionTest) {
    Columns columns;
    auto tc1 = DoubleColumn::create();
    tc1->append(-1);
    tc1->append(0);
    tc1->append(1);
    tc1->append(3.1415926);
    tc1->append(30);
    columns.emplace_back(tc1);

    {
        std::vector<double> result_expect = {std::sinh(-1), std::sinh(0), std::sinh(1), std::sinh(3.1415926),
                                             std::sinh(30)};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::sinh(ctx.get(), columns).value();
        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
        ASSERT_EQ(v->size(), result_expect.size());
        for (size_t i = 0; i < v->size(); i++) {
            ASSERT_EQ(v->get_data()[i], result_expect[i]);
        }
    }

    {
        std::vector<double> result_expect = {std::cosh(-1), std::cosh(0), std::cosh(1), std::cosh(3.1415926),
                                             std::cosh(30)};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::cosh(ctx.get(), columns).value();
        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
        ASSERT_EQ(v->size(), result_expect.size());
        for (size_t i = 0; i < v->size(); i++) {
            ASSERT_EQ(v->get_data()[i], result_expect[i]);
        }
    }

    {
        std::vector<double> result_expect = {std::tanh(-1), std::tanh(0), std::tanh(1), std::tanh(3.1415926),
                                             std::tanh(30)};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::tanh(ctx.get(), columns).value();
        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);
        ASSERT_EQ(v->size(), result_expect.size());
        for (size_t i = 0; i < v->size(); i++) {
            ASSERT_EQ(v->get_data()[i], result_expect[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, OutputNanTest) {
    Columns columns;
    auto tc1 = DoubleColumn::create();
    tc1->append(-10);
    tc1->append(1.0);
    tc1->append(std::nan("not a double number"));
    columns.emplace_back(tc1);

    {
        std::vector<bool> null_expect = {true, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::acos(ctx.get(), columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {false, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::sin(ctx.get(), columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {true, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::asin(ctx.get(), columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {false, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::sinh(ctx.get(), columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {false, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::cosh(ctx.get(), columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {false, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::tanh(ctx.get(), columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {true, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::log2(ctx.get(), columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        Columns binary_columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(-0.9);
        tc1->append(0.2);
        tc1->append(0.3);
        binary_columns.emplace_back(tc1);

        auto tc2 = DoubleColumn::create();
        tc2->append(0.8);
        tc2->append(0.2);
        tc2->append(0.3);
        binary_columns.emplace_back(tc2);

        std::vector<bool> null_expect = {true, false, false};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::pow(ctx.get(), binary_columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        Columns binary_columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(std::nan("not a double number"));
        tc1->append(0.2);
        tc1->append(0.3);
        binary_columns.emplace_back(tc1);

        auto tc2 = DoubleColumn::create();
        tc2->append(0.8);
        tc2->append(0.2);
        tc2->append(0.3);
        binary_columns.emplace_back(tc2);

        std::vector<bool> null_expect = {true, false, false};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::atan2(ctx.get(), binary_columns).value();
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }
}

} // namespace starrocks
