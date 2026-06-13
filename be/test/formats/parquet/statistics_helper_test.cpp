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

#include "formats/parquet/statistics_helper.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "formats/parquet/schema.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

class StatisticsHelperTest : public testing::Test {};

TEST_F(StatisticsHelperTest, DecodeBooleanMinMax) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);

    MutableColumnPtr min_column = ColumnHelper::create_column(type, true);
    MutableColumnPtr max_column = ColumnHelper::create_column(type, true);

    // Parquet stats encode BOOLEAN as a single byte: 0x00 = false, 0x01 = true.
    std::vector<std::string> min_values{std::string("\x00", 1), std::string("\x01", 1)};
    std::vector<std::string> max_values{std::string("\x01", 1), std::string("\x01", 1)};
    std::vector<bool> null_pages{false, false};

    ASSERT_TRUE(StatisticsHelper::decode_value_into_column(min_column, min_values, null_pages, type, &field, "UTC").ok());
    ASSERT_TRUE(StatisticsHelper::decode_value_into_column(max_column, max_values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(2, min_column->size());
    ASSERT_EQ(2, max_column->size());
    EXPECT_EQ(0, min_column->get(0).get_uint8());
    EXPECT_EQ(1, min_column->get(1).get_uint8());
    EXPECT_EQ(1, max_column->get(0).get_uint8());
    EXPECT_EQ(1, max_column->get(1).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanNullPage) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    std::vector<std::string> values{std::string(), std::string("\x01", 1)};
    std::vector<bool> null_pages{true, false};

    ASSERT_TRUE(StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC").ok());

    ASSERT_EQ(2, column->size());
    EXPECT_TRUE(column->is_null(0));
    EXPECT_FALSE(column->is_null(1));
    EXPECT_EQ(1, column->get(1).get_uint8());
}

TEST_F(StatisticsHelperTest, DecodeBooleanEmptyValueReturnsCorruption) {
    ParquetField field;
    field.physical_type = tparquet::Type::type::BOOLEAN;

    TypeDescriptor type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
    MutableColumnPtr column = ColumnHelper::create_column(type, true);

    std::vector<std::string> values{std::string()};
    std::vector<bool> null_pages{false};

    auto st = StatisticsHelper::decode_value_into_column(column, values, null_pages, type, &field, "UTC");
    EXPECT_TRUE(st.is_corruption()) << st.to_string();
}

} // namespace starrocks::parquet
