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
#include <vector>

#include "column/fixed_length_column.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

TEST(FixedLengthColumnCoreTest, CompareAtInt32) {
    std::vector<int32_t> numbers{1, 2, 3, 4, 5};
    auto lhs = Int32Column::create();
    auto rhs = Int32Column::create();
    lhs->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
    rhs->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));

    EXPECT_EQ(0, lhs->compare_at(0, 0, *rhs, -1));
    EXPECT_LT(lhs->compare_at(0, 4, *rhs, -1), 0);
    EXPECT_GT(lhs->compare_at(4, 0, *rhs, -1), 0);
}

TEST(FixedLengthColumnCoreTest, CompareAtDoubleWithNan) {
    auto lhs = DoubleColumn::create();
    auto rhs = DoubleColumn::create();
    const double nan = std::numeric_limits<double>::quiet_NaN();

    lhs->append(nan);
    lhs->append(nan);
    lhs->append(2.0);

    rhs->append(0.0);
    rhs->append(1.0);
    rhs->append(nan);

    EXPECT_EQ(0, lhs->compare_at(0, 0, *rhs, -1));
    EXPECT_LT(lhs->compare_at(1, 1, *rhs, -1), 0);
    EXPECT_GT(lhs->compare_at(2, 2, *rhs, -1), 0);
}

TEST(FixedLengthColumnCoreTest, DefaultValueDateTimestamp) {
    auto date_column = DateColumn::create();
    date_column->append_default(2);
    ASSERT_EQ(2, date_column->size());
    EXPECT_EQ(DateValue::MIN_DATE_VALUE, date_column->get(0).get_date());
    EXPECT_EQ(DateValue::MIN_DATE_VALUE, date_column->get(1).get_date());

    auto ts_column = TimestampColumn::create();
    ts_column->append_default(2);
    ASSERT_EQ(2, ts_column->size());
    EXPECT_EQ(TimestampValue::MIN_TIMESTAMP_VALUE, ts_column->get(0).get_timestamp());
    EXPECT_EQ(TimestampValue::MIN_TIMESTAMP_VALUE, ts_column->get(1).get_timestamp());
}

TEST(FixedLengthColumnCoreTest, FilterBasic) {
    auto column = Int32Column::create();
    for (int i = 0; i < 10; ++i) {
        column->append(i);
    }

    Filter filter(10, 0);
    for (size_t i = 0; i < filter.size(); ++i) {
        filter[i] = static_cast<uint8_t>(i % 2);
    }

    column->filter(filter);
    ASSERT_EQ(5, column->size());
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(2 * i + 1, column->get(i).get_int32());
    }
}

} // namespace starrocks
