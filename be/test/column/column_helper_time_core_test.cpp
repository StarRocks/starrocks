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

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "gtest/gtest.h"

namespace starrocks {

TEST(ColumnHelperTimeCoreTest, convertPlainDoubleTimeToString) {
    auto input = DoubleColumn::create();
    input->append(3661.0);
    input->append(-62.0);

    auto result = ColumnHelper::convert_time_column_from_double_to_str(input);
    ASSERT_TRUE(result->is_binary());
    auto* binary = ColumnHelper::as_raw_column<BinaryColumn>(result.get());
    EXPECT_EQ("01:01:01", binary->get_slice(0).to_string());
    EXPECT_EQ("-00:01:02", binary->get_slice(1).to_string());
}

TEST(ColumnHelperTimeCoreTest, convertNullableDoubleTimeToString) {
    auto input = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    input->append_datum(Datum(3600.0));
    input->append_nulls(1);

    auto result = ColumnHelper::convert_time_column_from_double_to_str(input);
    ASSERT_TRUE(result->is_nullable());
    auto* nullable = ColumnHelper::as_raw_column<NullableColumn>(result.get());
    ASSERT_FALSE(nullable->is_null(0));
    ASSERT_TRUE(nullable->is_null(1));

    auto* binary = ColumnHelper::as_raw_column<BinaryColumn>(nullable->data_column().get());
    EXPECT_EQ("01:00:00", binary->get_slice(0).to_string());
}

TEST(ColumnHelperTimeCoreTest, convertConstDoubleTimeToString) {
    auto data = DoubleColumn::create();
    data->append(7322.0);
    auto input = ConstColumn::create(std::move(data), 3);

    auto result = ColumnHelper::convert_time_column_from_double_to_str(input);
    ASSERT_TRUE(result->is_constant());
    EXPECT_EQ("02:02:02", result->get(0).get_slice().to_string());
    EXPECT_EQ(3, result->size());
}

} // namespace starrocks
