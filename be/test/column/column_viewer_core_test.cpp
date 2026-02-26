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

#include "column/column_helper.h"
#include "column/column_viewer.h"

namespace starrocks {

TEST(ColumnViewerCoreTest, ConstColumnMaskExpansion) {
    constexpr size_t logical_rows = 4;
    ColumnPtr const_col = ColumnHelper::create_const_column<TYPE_INT>(42, logical_rows);
    ColumnViewer<TYPE_INT> viewer(const_col);

    ASSERT_EQ(1, viewer.size());
    for (size_t i = 0; i < logical_rows; ++i) {
        EXPECT_FALSE(viewer.is_null(i));
        EXPECT_EQ(42, viewer.value(i));
    }
}

TEST(ColumnViewerCoreTest, NullableColumnUsesNullBitmap) {
    auto data = Int32Column::create();
    data->append(10);
    data->append(20);
    data->append(30);

    auto nulls = NullColumn::create();
    nulls->append(0);
    nulls->append(1);
    nulls->append(0);

    ColumnPtr nullable_col = NullableColumn::create(std::move(data), std::move(nulls));
    ColumnViewer<TYPE_INT> viewer(nullable_col);

    ASSERT_EQ(3, viewer.size());
    EXPECT_FALSE(viewer.is_null(0));
    EXPECT_TRUE(viewer.is_null(1));
    EXPECT_FALSE(viewer.is_null(2));
    EXPECT_EQ(10, viewer.value(0));
    EXPECT_EQ(30, viewer.value(2));
}

TEST(ColumnViewerCoreTest, NonNullableColumnNoNulls) {
    auto col = Int32Column::create();
    col->append(7);
    col->append(8);
    col->append(9);

    ColumnViewer<TYPE_INT> viewer(col);

    ASSERT_EQ(3, viewer.size());
    EXPECT_FALSE(viewer.is_null(0));
    EXPECT_FALSE(viewer.is_null(1));
    EXPECT_FALSE(viewer.is_null(2));
    EXPECT_EQ(7, viewer.value(0));
    EXPECT_EQ(8, viewer.value(1));
    EXPECT_EQ(9, viewer.value(2));
}

TEST(ColumnViewerCoreTest, OnlyNullColumnUsesSingletonMask) {
    constexpr size_t logical_rows = 5;
    ColumnPtr only_null_col = ColumnHelper::create_const_null_column(logical_rows);
    ColumnViewer<TYPE_INT> viewer(only_null_col);

    ASSERT_EQ(1, viewer.size());
    for (size_t i = 0; i < logical_rows; ++i) {
        EXPECT_TRUE(viewer.is_null(i));
        EXPECT_EQ(0, viewer.value(i));
    }
}

} // namespace starrocks
