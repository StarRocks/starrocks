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

#include "column/adaptive_nullable_column.h"

namespace starrocks {

TEST(AdaptiveNullableColumnCoreTest, NullStateTransitionAndNullCount) {
    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());

    EXPECT_EQ(AdaptiveNullableColumn::State::kUninitialized, col->state());
    EXPECT_EQ(0, col->size());

    ASSERT_TRUE(col->append_nulls(2));
    EXPECT_EQ(AdaptiveNullableColumn::State::kNull, col->state());
    EXPECT_EQ(2, col->size());
    EXPECT_EQ(2, col->null_count());
    EXPECT_TRUE(col->has_null());

    col->append_datum(Datum(int32_t(9)));
    EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
    EXPECT_EQ(3, col->size());
    EXPECT_TRUE(col->is_null(0));
    EXPECT_FALSE(col->is_null(2));
    EXPECT_EQ(9, col->get(2).get_int32());
}

TEST(AdaptiveNullableColumnCoreTest, NotConstantPathAndDefaultNotNullValue) {
    auto col = AdaptiveNullableColumn::create(Int32Column::create(), NullColumn::create());

    col->append_datum(Datum(int32_t(3)));
    EXPECT_EQ(AdaptiveNullableColumn::State::kNotConstant, col->state());
    EXPECT_EQ(1, col->size());
    EXPECT_FALSE(col->has_null());

    col->append_default_not_null_value();
    EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
    EXPECT_EQ(2, col->size());

    ASSERT_TRUE(col->append_nulls(1));
    EXPECT_EQ(AdaptiveNullableColumn::State::kMaterialized, col->state());
    EXPECT_TRUE(col->has_null());
    EXPECT_EQ(1, col->null_count());
}

} // namespace starrocks
