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

#include "column/type_traits.h"

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

TEST(TypeTraitsCoreTest, JsonLimits) {
    auto min_json = RunTimeTypeLimits<TYPE_JSON>::min_value();
    auto max_json = RunTimeTypeLimits<TYPE_JSON>::max_value();
    EXPECT_LT(min_json.compare(max_json), 0);
}

} // namespace starrocks
