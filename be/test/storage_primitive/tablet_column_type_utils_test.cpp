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

#include "storage/primitive/tablet_column_type_utils.h"

#include <cstdint>

#include "gtest/gtest.h"

namespace starrocks {

TEST(TabletColumnTypeUtilsTest, FixedLengthTypesKeepExistingWidths) {
    EXPECT_EQ(1, get_tablet_column_field_length_by_type(TYPE_BOOLEAN, 100));
    EXPECT_EQ(1, get_tablet_column_field_length_by_type(TYPE_TINYINT, 100));
    EXPECT_EQ(2, get_tablet_column_field_length_by_type(TYPE_SMALLINT, 100));
    EXPECT_EQ(3, get_tablet_column_field_length_by_type(TYPE_DATE_V1, 100));
    EXPECT_EQ(4, get_tablet_column_field_length_by_type(TYPE_INT, 100));
    EXPECT_EQ(4, get_tablet_column_field_length_by_type(TYPE_FLOAT, 100));
    EXPECT_EQ(8, get_tablet_column_field_length_by_type(TYPE_BIGINT, 100));
    EXPECT_EQ(8, get_tablet_column_field_length_by_type(TYPE_DOUBLE, 100));
    EXPECT_EQ(12, get_tablet_column_field_length_by_type(TYPE_DECIMAL, 100));
    EXPECT_EQ(16, get_tablet_column_field_length_by_type(TYPE_DECIMAL128, 100));
    EXPECT_EQ(32, get_tablet_column_field_length_by_type(TYPE_DECIMAL256, 100));
}

TEST(TabletColumnTypeUtilsTest, CharAndArrayUseProvidedLength) {
    EXPECT_EQ(12, get_tablet_column_field_length_by_type(TYPE_CHAR, 12));
    EXPECT_EQ(24, get_tablet_column_field_length_by_type(TYPE_ARRAY, 24));
}

TEST(TabletColumnTypeUtilsTest, VariableLengthTypesKeepLengthPrefixOverhead) {
    constexpr uint32_t kStringLength = 10;
    const uint32_t expected = kStringLength + sizeof(uint32_t);

    EXPECT_EQ(expected, get_tablet_column_field_length_by_type(TYPE_VARCHAR, kStringLength));
    EXPECT_EQ(expected, get_tablet_column_field_length_by_type(TYPE_HLL, kStringLength));
    EXPECT_EQ(expected, get_tablet_column_field_length_by_type(TYPE_PERCENTILE, kStringLength));
    EXPECT_EQ(expected, get_tablet_column_field_length_by_type(TYPE_JSON, kStringLength));
    EXPECT_EQ(expected, get_tablet_column_field_length_by_type(TYPE_VARIANT, kStringLength));
    EXPECT_EQ(expected, get_tablet_column_field_length_by_type(TYPE_VARBINARY, kStringLength));
}

} // namespace starrocks
