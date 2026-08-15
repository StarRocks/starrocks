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

#include <limits>
#include <type_traits>

#include "column/runtime_type_traits.h"

namespace starrocks {

static_assert(std::is_same_v<RunTimeTypeTraits<TYPE_VARCHAR>::ColumnType, BinaryColumn>);
static_assert(std::is_same_v<RunTimeTypeTraits<TYPE_DECIMAL64>::ColumnType, Decimal64Column>);
static_assert(std::is_same_v<RunTimeTypeTraits<TYPE_JSON>::ColumnType, JsonColumn>);
static_assert(isArithmeticLT<TYPE_INT>);
static_assert(!isArithmeticLT<TYPE_JSON>);
static_assert(isSliceLT<TYPE_VARCHAR>);

TEST(TypeTraitsCoreTest, RuntimeLimitsAndFixedLength) {
    EXPECT_EQ(std::numeric_limits<int32_t>::lowest(), RunTimeTypeLimits<TYPE_INT>::min_value());
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), RunTimeTypeLimits<TYPE_INT>::max_value());

    EXPECT_EQ(sizeof(int32_t), RunTimeFixedTypeLength<TYPE_INT>::value);
    EXPECT_EQ(0, RunTimeFixedTypeLength<TYPE_VARCHAR>::value);
}

TEST(TypeTraitsCoreTest, FixedLengthTypeSize) {
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_BOOLEAN>::value, get_size_of_fixed_length_type(TYPE_BOOLEAN));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_TINYINT>::value, get_size_of_fixed_length_type(TYPE_TINYINT));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_SMALLINT>::value, get_size_of_fixed_length_type(TYPE_SMALLINT));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_INT>::value, get_size_of_fixed_length_type(TYPE_INT));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_BIGINT>::value, get_size_of_fixed_length_type(TYPE_BIGINT));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_LARGEINT>::value, get_size_of_fixed_length_type(TYPE_LARGEINT));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_FLOAT>::value, get_size_of_fixed_length_type(TYPE_FLOAT));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DOUBLE>::value, get_size_of_fixed_length_type(TYPE_DOUBLE));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_TIME>::value, get_size_of_fixed_length_type(TYPE_TIME));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DECIMALV2>::value, get_size_of_fixed_length_type(TYPE_DECIMALV2));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DECIMAL32>::value, get_size_of_fixed_length_type(TYPE_DECIMAL32));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DECIMAL64>::value, get_size_of_fixed_length_type(TYPE_DECIMAL64));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DECIMAL128>::value, get_size_of_fixed_length_type(TYPE_DECIMAL128));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DECIMAL256>::value, get_size_of_fixed_length_type(TYPE_DECIMAL256));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DATE>::value, get_size_of_fixed_length_type(TYPE_DATE));
    EXPECT_EQ(RunTimeFixedTypeLength<TYPE_DATETIME>::value, get_size_of_fixed_length_type(TYPE_DATETIME));

    EXPECT_EQ(0, get_size_of_fixed_length_type(TYPE_VARCHAR));
    EXPECT_EQ(0, get_size_of_fixed_length_type(TYPE_JSON));
    EXPECT_EQ(0, get_size_of_fixed_length_type(TYPE_HLL));
    EXPECT_EQ(0, get_size_of_fixed_length_type(TYPE_ARRAY));
}

TEST(TypeTraitsCoreTest, JsonLimits) {
    auto min_json = RunTimeTypeLimits<TYPE_JSON>::min_value();
    auto max_json = RunTimeTypeLimits<TYPE_JSON>::max_value();
    EXPECT_LT(min_json.compare(max_json), 0);
}

} // namespace starrocks
