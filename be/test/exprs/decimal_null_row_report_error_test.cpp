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

// Under sql_mode=ERROR_IF_OVERFLOW (OverflowMode::REPORT_ERROR) a decimal cast or decimal arithmetic must only
// report an error for a non-null row. The data slot of a null row is undefined ("" for a string, 0, or a value
// left by an upstream expression), so it must never raise an error. Every case below builds one column whose
// null row holds a slot that the op rejects, next to a valid non-null row, so neither the all-null nor the
// no-null shortcut is taken.

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "exprs/arithmetic_operation.h"
#include "exprs/decimal_binary_function.h"
#include "exprs/decimal_cast_expr.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

namespace {

constexpr auto REPORT = OverflowMode::REPORT_ERROR;

template <typename DataColumn>
ColumnPtr make_nullable(typename DataColumn::MutablePtr data, const std::vector<uint8_t>& is_null) {
    auto nulls = NullColumn::create();
    bool any_null = false;
    for (auto n : is_null) {
        nulls->get_data().push_back(n);
        any_null |= (n != 0);
    }
    auto column = NullableColumn::create(std::move(data), std::move(nulls));
    column->set_has_null(any_null);
    return column;
}

ColumnPtr make_strings(const std::vector<std::string>& values, const std::vector<uint8_t>& is_null) {
    auto data = BinaryColumn::create();
    for (const auto& v : values) {
        data->append(Slice(v));
    }
    if (is_null.empty()) {
        return data;
    }
    return make_nullable<BinaryColumn>(std::move(data), is_null);
}

template <LogicalType Type>
ColumnPtr make_decimals(int precision, int scale, const std::vector<RunTimeCppType<Type>>& values,
                        const std::vector<uint8_t>& is_null) {
    auto data = RunTimeColumnType<Type>::create(precision, scale);
    for (const auto& v : values) {
        data->get_data().push_back(v);
    }
    if (is_null.empty()) {
        return data;
    }
    return make_nullable<RunTimeColumnType<Type>>(std::move(data), is_null);
}

template <LogicalType Type>
ColumnPtr make_values(const std::vector<RunTimeCppType<Type>>& values, const std::vector<uint8_t>& is_null) {
    auto data = RunTimeColumnType<Type>::create();
    for (const auto& v : values) {
        data->get_data().push_back(v);
    }
    return make_nullable<RunTimeColumnType<Type>>(std::move(data), is_null);
}

void expect_nulls(const ColumnPtr& result, const std::vector<bool>& expected) {
    ASSERT_EQ(result->size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(result->is_null(i), expected[i]) << "row " << i;
    }
}

template <LogicalType Type>
RunTimeCppType<Type> value_at(const ColumnPtr& result, size_t row) {
    const auto* data = ColumnHelper::get_data_column(result.get());
    return down_cast<const RunTimeColumnType<Type>*>(data)->immutable_data()[row];
}

template <LogicalType From, LogicalType To, typename... Args>
ColumnPtr cast_from(const ColumnPtr& column, Args... args) {
    return VectorizedDecimalCastFunction<DecimalFrom, REPORT>::evaluate<From, To>(column, args...);
}

template <LogicalType From, LogicalType To>
ColumnPtr cast_to(const ColumnPtr& column) {
    return VectorizedDecimalCastFunction<DecimalTo, REPORT>::evaluate<From, To>(column);
}

template <LogicalType From, LogicalType To>
ColumnPtr cast_decimal(const ColumnPtr& column, int precision, int scale) {
    return VectorizedDecimalCastFunction<DecimalToDecimal, REPORT>::evaluate<From, To>(column, precision, scale);
}

const int128_t kE20 = static_cast<int128_t>(100000000000LL) * 1000000000LL; // 1e20
const int128_t kE38 = kE20 * 1000000000000000000LL;                         // 1e38

} // namespace

class DecimalNullRowReportErrorTest : public ::testing::Test {
public:
    void SetUp() override { date::init_date_cache(); }
};

// ---------------------------------------------------------------- cast -> DECIMAL (DecimalFrom)

// Issue #82140: NULLIF(x, '') leaves "" in the null row, and parsing "" fails.
TEST_F(DecimalNullRowReportErrorTest, string_to_decimal_ignores_null_row) {
    auto input = make_strings({"", "1.25"}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_from<TYPE_VARCHAR, TYPE_DECIMAL128>(input, 38, 18)));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_DECIMAL128>(result, 1), static_cast<int128_t>(1250000000000000000LL));
}

TEST_F(DecimalNullRowReportErrorTest, cast_result_owns_its_null_column) {
    auto input = make_strings({"", "1.25"}, {1, 0});
    ColumnPtr result = cast_from<TYPE_VARCHAR, TYPE_DECIMAL128>(input, 38, 18);
    const auto* in = down_cast<const NullableColumn*>(input.get());
    auto* out = down_cast<NullableColumn*>(result->as_mutable_raw_ptr());
    ASSERT_NE(in->null_column().get(), out->null_column().get());
    out->set_null(1);
    EXPECT_FALSE(input->is_null(1));
}

TEST_F(DecimalNullRowReportErrorTest, string_to_decimal_reports_non_null_row) {
    // the bad value sits in a non-null row next to a null row: the original error is still raised
    auto input = make_strings({"", "abc", "1"}, {1, 0, 0});
    try {
        (void)cast_from<TYPE_VARCHAR, TYPE_DECIMAL64>(input, 18, 2);
        FAIL() << "expected an overflow error";
    } catch (const std::overflow_error& e) {
        EXPECT_STREQ(e.what(), "The type cast from string to decimal overflows");
    }
    // no null rows: the strict path is unchanged
    EXPECT_THROW((cast_from<TYPE_VARCHAR, TYPE_DECIMAL64>(make_strings({"abc"}, {}), 18, 2)), std::overflow_error);
    EXPECT_NO_THROW((cast_from<TYPE_VARCHAR, TYPE_DECIMAL64>(make_strings({"1.25"}, {}), 18, 2)));
}

TEST_F(DecimalNullRowReportErrorTest, bigint_to_decimal_ignores_null_row) {
    auto input = make_values<TYPE_BIGINT>({10000000000LL, 1}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_from<TYPE_BIGINT, TYPE_DECIMAL32>(input, 9, 0)));
    expect_nulls(result, {true, false});
    EXPECT_THROW((cast_from<TYPE_BIGINT, TYPE_DECIMAL32>(make_values<TYPE_BIGINT>({10000000000LL, 1}, {0, 1}), 9, 0)),
                 std::overflow_error);
}

TEST_F(DecimalNullRowReportErrorTest, double_to_decimal_ignores_null_row) {
    auto input = make_values<TYPE_DOUBLE>({1e300, 1.5}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_from<TYPE_DOUBLE, TYPE_DECIMAL64>(input, 18, 2)));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_DECIMAL64>(result, 1), 150);
}

TEST_F(DecimalNullRowReportErrorTest, date_to_decimal_ignores_null_row) {
    // 9999-12-31 -> 99991231 * 100 overflows DECIMAL32(9,2); 2024-01-01 -> 2024010100 fits
    auto input = make_values<TYPE_DATE>({DateValue::create(9999, 12, 31), DateValue::create(2024, 1, 1)}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_from<TYPE_DATE, TYPE_DECIMAL32>(input, 9, 2)));
    expect_nulls(result, {true, false});
    EXPECT_THROW(
            (cast_from<TYPE_DATE, TYPE_DECIMAL32>(
                    make_values<TYPE_DATE>({DateValue::create(9999, 12, 31), DateValue::create(2024, 1, 1)}, {0, 1}), 9,
                    2)),
            std::overflow_error);
}

TEST_F(DecimalNullRowReportErrorTest, datetime_to_decimal_ignores_null_row) {
    // 99991231235959 * 10^5 overflows DECIMAL64(18,5); 20240101000000 * 10^5 fits
    auto input = make_values<TYPE_DATETIME>(
            {TimestampValue::create(9999, 12, 31, 23, 59, 59), TimestampValue::create(2024, 1, 1, 0, 0, 0)}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_from<TYPE_DATETIME, TYPE_DECIMAL64>(input, 18, 5)));
    expect_nulls(result, {true, false});
}

TEST_F(DecimalNullRowReportErrorTest, decimalv2_to_decimal_ignores_null_row) {
    auto input = make_values<TYPE_DECIMALV2>({DecimalV2Value("100000000"), DecimalV2Value("1")}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_from<TYPE_DECIMALV2, TYPE_DECIMAL32>(input, 9, 2)));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_DECIMAL32>(result, 1), 100);
}

// ---------------------------------------------------------------- DECIMAL -> DECIMAL (DecimalToDecimal)

TEST_F(DecimalNullRowReportErrorTest, decimal_to_decimal_ignores_null_row) {
    // narrowing, same scale
    auto narrow = make_decimals<TYPE_DECIMAL128>(38, 0, {kE20, 1}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_decimal<TYPE_DECIMAL128, TYPE_DECIMAL64>(narrow, 18, 0)));
    expect_nulls(result, {true, false});
    // scale up
    auto up = make_decimals<TYPE_DECIMAL128>(20, 0, {static_cast<int128_t>(10000000000LL), 1}, {1, 0});
    ASSERT_NO_THROW(result = (cast_decimal<TYPE_DECIMAL128, TYPE_DECIMAL128>(up, 38, 30)));
    expect_nulls(result, {true, false});
    // scale down
    auto down = make_decimals<TYPE_DECIMAL128>(38, 10, {kE20 * 10000000000LL, 10000000000LL}, {1, 0});
    ASSERT_NO_THROW(result = (cast_decimal<TYPE_DECIMAL128, TYPE_DECIMAL64>(down, 18, 2)));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_DECIMAL64>(result, 1), 100);
    // a non-null overflowing row is still reported
    EXPECT_THROW((cast_decimal<TYPE_DECIMAL128, TYPE_DECIMAL64>(
                         make_decimals<TYPE_DECIMAL128>(38, 0, {kE20, 1}, {0, 1}), 18, 0)),
                 std::overflow_error);
}

// ---------------------------------------------------------------- DECIMAL -> other (DecimalTo)

TEST_F(DecimalNullRowReportErrorTest, decimal_to_number_and_time_ignore_null_row) {
    auto input = make_decimals<TYPE_DECIMAL64>(18, 0, {1000, 1}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_to<TYPE_DECIMAL64, TYPE_TINYINT>(input)));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_TINYINT>(result, 1), 1);

    auto time_input = make_decimals<TYPE_DECIMAL64>(18, 0, {99, 1}, {1, 0}); // 99 seconds is not a valid TIME
    ASSERT_NO_THROW(result = (cast_to<TYPE_DECIMAL64, TYPE_TIME>(time_input)));
    expect_nulls(result, {true, false});

    EXPECT_THROW((cast_to<TYPE_DECIMAL64, TYPE_TINYINT>(make_decimals<TYPE_DECIMAL64>(18, 0, {1000, 1}, {0, 1}))),
                 std::overflow_error);
}

TEST_F(DecimalNullRowReportErrorTest, decimal_to_date_and_decimalv2_ignore_null_row) {
    // the default slot 0 is not a valid date literal
    auto date_input = make_decimals<TYPE_DECIMAL64>(18, 0, {0, 20240101}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = (cast_to<TYPE_DECIMAL64, TYPE_DATE>(date_input)));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_DATE>(result, 1), DateValue::create(2024, 1, 1));

    auto datetime_input = make_decimals<TYPE_DECIMAL64>(18, 0, {0, 20240101000000LL}, {1, 0});
    ASSERT_NO_THROW(result = (cast_to<TYPE_DECIMAL64, TYPE_DATETIME>(datetime_input)));
    expect_nulls(result, {true, false});

    auto v2_input = make_decimals<TYPE_DECIMAL128>(38, 0, {kE38, 1}, {1, 0});
    ASSERT_NO_THROW(result = (cast_to<TYPE_DECIMAL128, TYPE_DECIMALV2>(v2_input)));
    expect_nulls(result, {true, false});
}

// ---------------------------------------------------------------- decimal arithmetic

TEST_F(DecimalNullRowReportErrorTest, div_and_mod_ignore_null_divisor) {
    using Div = VectorizedUnstrictDecimalBinaryFunction<TYPE_DECIMAL128, DivOp, REPORT>;
    using Mod = VectorizedUnstrictDecimalBinaryFunction<TYPE_DECIMAL128, ModOp, REPORT>;
    // the null divisor's slot holds the default 0
    auto lhs = make_decimals<TYPE_DECIMAL128>(38, 0, {1, 3}, {});
    auto rhs = make_decimals<TYPE_DECIMAL128>(38, 0, {0, 2}, {1, 0});
    ColumnPtr result;
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    expect_nulls(result, {true, false});
    ASSERT_NO_THROW(result = Mod::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_DECIMAL128>(result, 1), 1);

    // const lhs / nullable rhs
    auto const_lhs = ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 0, {1}, {}), 2);
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(const_lhs, rhs));
    expect_nulls(result, {true, false});
}

// A zero divisor yields NULL as in the other overflow modes, instead of the overflow of the placeholder quotient.
TEST_F(DecimalNullRowReportErrorTest, div_and_mod_by_zero_yield_null) {
    using Div = VectorizedUnstrictDecimalBinaryFunction<TYPE_DECIMAL128, DivOp, REPORT>;
    using Mod = VectorizedUnstrictDecimalBinaryFunction<TYPE_DECIMAL128, ModOp, REPORT>;
    const int128_t big = kE38 / 10 * 9; // 9e37
    ColumnPtr result;

    // non-nullable operands, small and large dividends
    auto lhs = make_decimals<TYPE_DECIMAL128>(38, 2, {100, big, 300}, {});
    auto rhs = make_decimals<TYPE_DECIMAL128>(38, 2, {0, 0, 200}, {});
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    expect_nulls(result, {true, true, false});
    ASSERT_NO_THROW(result = Mod::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    expect_nulls(result, {true, true, false});
    EXPECT_EQ(value_at<TYPE_DECIMAL128>(result, 2), 100);

    // nullable operands: a NULL row next to a zero divisor
    auto nullable_lhs = make_decimals<TYPE_DECIMAL128>(38, 2, {100, big, 300}, {1, 0, 0});
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(nullable_lhs, rhs));
    expect_nulls(result, {true, true, false});

    // a constant zero divisor
    auto const_zero = ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 2, {0}, {}), 3);
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(lhs, const_zero));
    expect_nulls(result, {true, true, true});
    ASSERT_NO_THROW(result = Mod::evaluate<TYPE_DECIMAL128>(lhs, const_zero));
    expect_nulls(result, {true, true, true});

    // a constant dividend whose scale-up overflows, over zero divisors only; and constant over constant
    auto const_big = ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 2, {big}, {}), 3);
    auto zeros = make_decimals<TYPE_DECIMAL128>(38, 2, {0, 0, 0}, {});
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(const_big, zeros));
    expect_nulls(result, {true, true, true});
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(const_big, const_zero));
    expect_nulls(result, {true, true, true});
    auto const_one = ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 2, {100}, {}), 3);
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(const_one, rhs));
    expect_nulls(result, {true, true, false});

    // a real overflow in a row with a non-zero divisor is still reported
    auto one_cent = make_decimals<TYPE_DECIMAL128>(38, 2, {0, 1}, {});
    auto big_lhs = make_decimals<TYPE_DECIMAL128>(38, 2, {big, big}, {});
    try {
        (void)Div::evaluate<TYPE_DECIMAL128>(big_lhs, one_cent);
        FAIL() << "expected an overflow error";
    } catch (const std::overflow_error& e) {
        EXPECT_STREQ(e.what(), "The 'div' operation involving decimal values overflows");
    }
}

TEST_F(DecimalNullRowReportErrorTest, add_sub_mul_ignore_null_row) {
    using Add = VectorizedStrictDecimalBinaryFunction<AddOp, REPORT>;
    using Sub = VectorizedStrictDecimalBinaryFunction<SubOp, REPORT>;
    using Mul = VectorizedStrictDecimalBinaryFunction<MulOp, REPORT>;
    const int128_t big = kE38 / 10 * 9; // 9e37
    // the null row of lhs holds a value left by an upstream `a + b`
    auto lhs = make_decimals<TYPE_DECIMAL128>(38, 0, {big, 1}, {1, 0});
    auto rhs = make_decimals<TYPE_DECIMAL128>(38, 0, {big, 1}, {});
    auto neg_rhs = make_decimals<TYPE_DECIMAL128>(38, 0, {-big, 1}, {});
    ColumnPtr result;
    ASSERT_NO_THROW(result = Add::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    expect_nulls(result, {true, false});
    EXPECT_EQ(value_at<TYPE_DECIMAL128>(result, 1), 2);
    ASSERT_NO_THROW(result = Sub::evaluate<TYPE_DECIMAL128>(lhs, neg_rhs));
    expect_nulls(result, {true, false});
    ASSERT_NO_THROW(result = Mul::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    expect_nulls(result, {true, false});

    // nullable lhs + const rhs
    auto const_rhs = ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 0, {big}, {}), 2);
    ASSERT_NO_THROW(result = Add::evaluate<TYPE_DECIMAL128>(lhs, const_rhs));
    expect_nulls(result, {true, false});

    // a non-null overflowing row is still reported
    auto bad_lhs = make_decimals<TYPE_DECIMAL128>(38, 0, {big, 1}, {0, 1});
    try {
        (void)Add::evaluate<TYPE_DECIMAL128>(bad_lhs, rhs);
        FAIL() << "expected an overflow error";
    } catch (const std::overflow_error& e) {
        EXPECT_STREQ(e.what(), "The 'add' operation involving decimal values overflows");
    }
}

// A decimal const-op-const result under REPORT_ERROR is a one-row ConstColumn, which can then meet a full-size
// nullable operand, e.g. `(if(k > 0, 1.0, 2.0) + 1.0) + d` when IF keeps one constant branch for the chunk.
TEST_F(DecimalNullRowReportErrorTest, one_row_const_lhs_with_nullable_rhs) {
    using Add = VectorizedStrictDecimalBinaryFunction<AddOp, REPORT>;
    using Div = VectorizedUnstrictDecimalBinaryFunction<TYPE_DECIMAL128, DivOp, REPORT>;
    constexpr size_t kRows = 64;
    auto one = [&] { return ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 0, {1}, {}), kRows); };
    ColumnPtr lhs = Add::evaluate<TYPE_DECIMAL128>(one(), one());
    ASSERT_TRUE(lhs->is_constant());
    ASSERT_EQ(1, lhs->size());

    std::vector<int128_t> values(kRows, 3);
    std::vector<uint8_t> is_null(kRows, 0);
    std::vector<bool> expected(kRows, false);
    for (size_t i = 0; i < kRows; i += 2) {
        values[i] = 0; // a null divisor holds 0
        is_null[i] = 1;
        expected[i] = true;
    }
    auto rhs = make_decimals<TYPE_DECIMAL128>(38, 0, values, is_null);

    ColumnPtr result;
    ASSERT_NO_THROW(result = Add::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    ASSERT_EQ(kRows, result->size());
    expect_nulls(result, expected);
    EXPECT_EQ(value_at<TYPE_DECIMAL128>(result, 1), 5);
    ASSERT_NO_THROW(result = Div::evaluate<TYPE_DECIMAL128>(lhs, rhs));
    ASSERT_EQ(kRows, result->size());
    expect_nulls(result, expected);

    // a non-null overflowing row is still reported
    const int128_t big = kE38 / 10 * 9; // 9e37
    ColumnPtr big_lhs = Add::evaluate<TYPE_DECIMAL128>(
            ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 0, {big}, {}), kRows), one());
    ASSERT_EQ(1, big_lhs->size());
    values[1] = big;
    auto bad_rhs = make_decimals<TYPE_DECIMAL128>(38, 0, values, is_null);
    try {
        (void)Add::evaluate<TYPE_DECIMAL128>(big_lhs, bad_rhs);
        FAIL() << "expected an overflow error";
    } catch (const std::overflow_error& e) {
        EXPECT_STREQ(e.what(), "The 'add' operation involving decimal values overflows");
    }
}

// The kernel scales a constant operand up once, before its row loop, so that overflow is not tied to a row. It must
// not raise an error when every row of the other operand is NULL, and must when any row is non-null.
TEST_F(DecimalNullRowReportErrorTest, const_operand_scale_up_overflow) {
    using Add = VectorizedStrictDecimalBinaryFunction<AddOp, REPORT>;
    const int128_t big = kE38 / 10 * 9; // 9e37, overflows once scaled up by 2 digits
    auto big_lhs = [&] { return ConstColumn::create(make_decimals<TYPE_DECIMAL128>(38, 0, {big}, {}), 3); };

    auto all_null_rhs = make_decimals<TYPE_DECIMAL128>(38, 2, {0, 0, 0}, {1, 1, 1});
    ColumnPtr result;
    ASSERT_NO_THROW(result = Add::evaluate<TYPE_DECIMAL128>(big_lhs(), all_null_rhs));
    expect_nulls(result, {true, true, true});

    auto rhs = make_decimals<TYPE_DECIMAL128>(38, 2, {0, 100, 0}, {1, 0, 1});
    try {
        (void)Add::evaluate<TYPE_DECIMAL128>(big_lhs(), rhs);
        FAIL() << "expected an overflow error";
    } catch (const std::overflow_error& e) {
        EXPECT_STREQ(e.what(), "The 'add' operation involving decimal values overflows");
    }
}

} // namespace starrocks
