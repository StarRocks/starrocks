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

#include "runtime/time_types.h"

#include <gtest/gtest.h>

namespace starrocks {

class TimeTypesTest : public testing::Test {
public:
    TimeTypesTest() = default;

protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test parsing datetime strings without separators (interval format)
TEST_F(TimeTypesTest, from_string_to_datetime_interval_format) {
    date::ToDatetimeResult res;

    // Test YYYYMMDDHHMMSS format (14 digits)
    {
        const char* date_str = "20231225123456";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        EXPECT_TRUE(is_valid);
        EXPECT_FALSE(is_only_date); // Should be parsed as full datetime, not date-only
        EXPECT_EQ(2023, res.year);
        EXPECT_EQ(12, res.month);
        EXPECT_EQ(25, res.day);
        EXPECT_EQ(12, res.hour);
        EXPECT_EQ(34, res.minute);
        EXPECT_EQ(56, res.second);
    }

    // Test YYYYMMDD format (8 digits) - should be date-only
    {
        const char* date_str = "20231225";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        EXPECT_TRUE(is_valid);
        EXPECT_TRUE(is_only_date); // 8 digits is date-only
        EXPECT_EQ(2023, res.year);
        EXPECT_EQ(12, res.month);
        EXPECT_EQ(25, res.day);
    }

    // Test YYYYMMDDTHHMMSS format (with T separator)
    {
        const char* date_str = "20231225T123456";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        EXPECT_TRUE(is_valid);
        EXPECT_FALSE(is_only_date);
        EXPECT_EQ(2023, res.year);
        EXPECT_EQ(12, res.month);
        EXPECT_EQ(25, res.day);
        EXPECT_EQ(12, res.hour);
        EXPECT_EQ(34, res.minute);
        EXPECT_EQ(56, res.second);
    }
}

// Test parsing standard datetime strings with separators
TEST_F(TimeTypesTest, from_string_to_datetime_standard_format) {
    date::ToDatetimeResult res;

    // Test YYYY-MM-DD HH:MM:SS format
    {
        const char* date_str = "2023-12-25 12:34:56";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        EXPECT_TRUE(is_valid);
        EXPECT_FALSE(is_only_date);
        EXPECT_EQ(2023, res.year);
        EXPECT_EQ(12, res.month);
        EXPECT_EQ(25, res.day);
        EXPECT_EQ(12, res.hour);
        EXPECT_EQ(34, res.minute);
        EXPECT_EQ(56, res.second);
    }

    // Test YYYY-MM-DD format (date-only)
    {
        const char* date_str = "2023-12-25";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        EXPECT_TRUE(is_valid);
        EXPECT_TRUE(is_only_date);
        EXPECT_EQ(2023, res.year);
        EXPECT_EQ(12, res.month);
        EXPECT_EQ(25, res.day);
    }

    // Test YYYY-MM-DDTHH:MM:SS format (ISO 8601)
    {
        const char* date_str = "2023-12-25T12:34:56";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        EXPECT_TRUE(is_valid);
        EXPECT_FALSE(is_only_date);
        EXPECT_EQ(2023, res.year);
        EXPECT_EQ(12, res.month);
        EXPECT_EQ(25, res.day);
        EXPECT_EQ(12, res.hour);
        EXPECT_EQ(34, res.minute);
        EXPECT_EQ(56, res.second);
    }
}

// Test edge cases where the function should fallback to generic parser
TEST_F(TimeTypesTest, from_string_to_datetime_edge_cases) {
    date::ToDatetimeResult res;

    // Test string with date part but insufficient time part chars
    {
        const char* date_str = "2023-12-25 12";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        // Generic parser should handle this
        EXPECT_TRUE(is_valid);
    }

    // Test string with no separator after date
    {
        const char* date_str = "2023-12-25X";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(date_str, strlen(date_str), &res);
        // Should fallback to generic parser
        // Generic parser might reject this, but at least we tried
    }
}

} // namespace starrocks
