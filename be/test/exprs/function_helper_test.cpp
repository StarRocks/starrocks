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

#include "exprs/function_helper.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {
struct FunctionHelperTest : public ::testing::Test {};
TEST_F(FunctionHelperTest, testMergeOnlyNullColumnMergeWithNullColumn) {
    auto only_null_column = ColumnHelper::create_const_null_column(10);
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column =
            FunctionHelper::merge_column_and_null_column(std::move(only_null_column), std::move(null_column));
    ASSERT_TRUE(result_column->only_null());
    ASSERT_EQ(result_column->size(), 10);
}

TEST_F(FunctionHelperTest, testMergeConstColumnMergeWithNullColumn) {
    auto data_column = Decimal128Column::create(38, 10);
    data_column->append((int128_t)111);
    auto const_column = ConstColumn::create(std::move(data_column), 10);
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column = FunctionHelper::merge_column_and_null_column(std::move(const_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}

TEST_F(FunctionHelperTest, testMergeNullableColumnMergeWithNullColumn) {
    auto nullable_column = NullableColumn::create(Decimal128Column::create(38, 10), NullColumn::create());
    nullable_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        if (i % 3 == 0) {
            nullable_column->append_nulls(1);
        } else {
            Datum datum((int128_t)111);
            nullable_column->append_datum(datum);
        }
    }
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column =
            FunctionHelper::merge_column_and_null_column(std::move(nullable_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0 && i % 3 != 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}
TEST_F(FunctionHelperTest, testMergeNotNullableColumnMergeWithNullColumn) {
    auto data_column = Decimal128Column::create(38, 10);
    data_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        data_column->append((int128_t)111);
    }
    auto null_column = NullColumn::create();
    null_column->reserve(10);
    for (int i = 0; i < 10; ++i) {
        null_column->append(i % 2 == 0 ? DATUM_NOT_NULL : DATUM_NULL);
    }
    auto result_column = FunctionHelper::merge_column_and_null_column(std::move(data_column), std::move(null_column));
    ASSERT_TRUE(result_column->is_nullable());
    ASSERT_EQ(result_column->size(), 10);
    auto* result_nullable_column = down_cast<NullableColumn*>(result_column.get());
    auto* result_data_column = down_cast<Decimal128Column*>(result_nullable_column->data_column().get());
    auto& result_data = result_data_column->get_data();
    for (int i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            ASSERT_FALSE(result_nullable_column->is_null(i));
            ASSERT_EQ(result_data[i], (int128_t)111);
        } else {
            ASSERT_TRUE(result_nullable_column->is_null(i));
        }
    }
}
} // namespace starrocks
