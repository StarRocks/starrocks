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

#include "exprs/unary_function.h"

#include <gtest/gtest.h>

#include <stdexcept>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "types/logical_type.h"
#include "util/numeric_types.h"

namespace starrocks {

// Ops mirroring cast_expr.cpp's ImplicitToNumber / NumberCheck / NumberCheckWithThrowException so
// the test exercises NullAwareInputCheckUnaryFunction exactly the way a strict-mode cast does.
DEFINE_UNARY_FN_WITH_IMPL(TestToNumber, value) {
    return value;
}

DEFINE_UNARY_FN_WITH_IMPL(TestOverflowCheck, value) {
    return check_signed_number_overflow<Type, ResultType>(value);
}

DEFINE_UNARY_FN_WITH_IMPL(TestOverflowCheckThrow, value) {
    const auto overflow = TestOverflowCheck::apply<Type, ResultType>(value);
    if (overflow) {
        throw std::runtime_error("cast overflow");
    }
    return overflow;
}

// The third (non-throwing) parameter is the one that keeps the hot loop branchless; the first two
// match the production strict-cast wiring.
using Caster = NullAwareInputCheckUnaryFunction<TestToNumber, TestOverflowCheckThrow, TestOverflowCheck>;

class NullAwareInputCheckUnaryFunctionTest : public ::testing::Test {
protected:
    static constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();
    static constexpr int64_t kOverflow = static_cast<int64_t>(kInt32Max) + 100; // out of INT range

    static ColumnPtr cast(const ColumnPtr& column) { return Caster::evaluate<TYPE_BIGINT, TYPE_INT>(column); }

    static ColumnPtr make_bigint(const std::vector<int64_t>& values) {
        auto col = RunTimeColumnType<TYPE_BIGINT>::create();
        col->get_data().assign(values.begin(), values.end());
        return col;
    }

    // Build a nullable BIGINT column from (value, is_null) pairs.
    static ColumnPtr make_nullable_bigint(const std::vector<std::pair<int64_t, bool>>& rows) {
        auto data = RunTimeColumnType<TYPE_BIGINT>::create();
        auto nulls = NullColumn::create();
        bool any_null = false;
        for (const auto& [value, is_null] : rows) {
            data->get_data().push_back(value);
            nulls->get_data().push_back(is_null ? 1 : 0);
            any_null |= is_null;
        }
        auto col = NullableColumn::create(std::move(data), std::move(nulls));
        col->set_has_null(any_null);
        return col;
    }
};

// Non-nullable input, all values in range: straight conversion, no exception, no nulls.
TEST_F(NullAwareInputCheckUnaryFunctionTest, NonNullableInRange) {
    auto input = make_bigint({1, -5, 100, kInt32Max, -kInt32Max});
    ColumnPtr result;
    ASSERT_NO_THROW(result = cast(input));

    ASSERT_EQ(5, result->size());
    EXPECT_FALSE(result->has_null());
    EXPECT_EQ(1, result->get(0).get_int32());
    EXPECT_EQ(-5, result->get(1).get_int32());
    EXPECT_EQ(100, result->get(2).get_int32());
    EXPECT_EQ(kInt32Max, result->get(3).get_int32());
    EXPECT_EQ(-kInt32Max, result->get(4).get_int32());
}

// Non-nullable input with an out-of-range value: must throw.
TEST_F(NullAwareInputCheckUnaryFunctionTest, NonNullableOverflowThrows) {
    auto input = make_bigint({1, kOverflow});
    EXPECT_THROW((void)cast(input), std::runtime_error);
}

// Nullable input that holds no null: behaves like the non-nullable case (has_null() fast path).
TEST_F(NullAwareInputCheckUnaryFunctionTest, NullableNoNullInRange) {
    auto input = make_nullable_bigint({{1, false}, {2, false}, {3, false}});
    ColumnPtr result;
    ASSERT_NO_THROW(result = cast(input));

    ASSERT_EQ(3, result->size());
    EXPECT_FALSE(result->has_null());
    EXPECT_EQ(1, result->get(0).get_int32());
    EXPECT_EQ(2, result->get(1).get_int32());
    EXPECT_EQ(3, result->get(2).get_int32());
}

// Nullable input with nulls, all non-null values in range: nulls preserved, no exception.
TEST_F(NullAwareInputCheckUnaryFunctionTest, NullableWithNullsInRange) {
    auto input = make_nullable_bigint({{10, true}, {20, false}, {30, true}, {40, false}});
    ColumnPtr result;
    ASSERT_NO_THROW(result = cast(input));

    ASSERT_EQ(4, result->size());
    ASSERT_TRUE(result->has_null());
    EXPECT_TRUE(result->get(0).is_null());
    EXPECT_EQ(20, result->get(1).get_int32());
    EXPECT_TRUE(result->get(2).is_null());
    EXPECT_EQ(40, result->get(3).get_int32());
}

// Core regression: a null row whose (undefined) data is out of range must NOT trigger an exception,
// as long as every non-null row is in range.
TEST_F(NullAwareInputCheckUnaryFunctionTest, NullRowGarbageOverflowDoesNotThrow) {
    auto input = make_nullable_bigint({{kOverflow, true}, {42, false}, {kOverflow, true}, {7, false}});
    ColumnPtr result;
    ASSERT_NO_THROW(result = cast(input));

    ASSERT_EQ(4, result->size());
    ASSERT_TRUE(result->has_null());
    EXPECT_TRUE(result->get(0).is_null());
    EXPECT_EQ(42, result->get(1).get_int32());
    EXPECT_TRUE(result->get(2).is_null());
    EXPECT_EQ(7, result->get(3).get_int32());
}

// A genuinely non-null row that overflows must still throw, even when other rows are null.
TEST_F(NullAwareInputCheckUnaryFunctionTest, NonNullRowOverflowThrows) {
    auto input = make_nullable_bigint({{0, true}, {kOverflow, false}, {1, false}});
    EXPECT_THROW((void)cast(input), std::runtime_error);
}

// only-null constant input is returned untouched, even though its data is undefined.
TEST_F(NullAwareInputCheckUnaryFunctionTest, OnlyNull) {
    auto input = ColumnHelper::create_const_null_column(4);
    ColumnPtr result;
    ASSERT_NO_THROW(result = cast(input));

    EXPECT_TRUE(result->only_null());
    EXPECT_EQ(4, result->size());
}

// Constant non-null input in range: result is a constant column with the converted value.
TEST_F(NullAwareInputCheckUnaryFunctionTest, ConstInRange) {
    auto input = ColumnHelper::create_const_column<TYPE_BIGINT>(123, 5);
    ColumnPtr result;
    ASSERT_NO_THROW(result = cast(input));

    ASSERT_EQ(5, result->size());
    EXPECT_TRUE(result->is_constant());
    EXPECT_EQ(123, result->get(0).get_int32());
    EXPECT_EQ(123, result->get(4).get_int32());
}

// Constant non-null input out of range: must throw.
TEST_F(NullAwareInputCheckUnaryFunctionTest, ConstOverflowThrows) {
    auto input = ColumnHelper::create_const_column<TYPE_BIGINT>(kOverflow, 5);
    EXPECT_THROW((void)cast(input), std::runtime_error);
}

} // namespace starrocks