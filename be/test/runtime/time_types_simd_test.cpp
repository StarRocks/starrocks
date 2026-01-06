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

#include "runtime/time_types.h"

namespace starrocks {

// Test for SIMD-optimized datetime parsing with invalid time components
class TimestampSIMDParsingTest : public ::testing::Test {};

TEST_F(TimestampSIMDParsingTest, test_invalid_minute_values) {
    date::ToDatetimeResult res;

    // Test minute = 60 (invalid)
    {
        const char* str = "2024-01-01 01:60:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "minute=60 should be invalid";
    }

    // Test minute = 61 (invalid)
    {
        const char* str = "2024-01-01 01:61:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "minute=61 should be invalid";
    }

    // Test minute = 99 (invalid)
    {
        const char* str = "2024-01-01 01:99:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "minute=99 should be invalid";
    }

    // Test minute = 59 (valid)
    {
        const char* str = "2024-01-01 01:59:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_TRUE(is_valid) << "minute=59 should be valid";
        EXPECT_FALSE(is_only_date) << "should include time component";
        EXPECT_EQ(2024, res.year);
        EXPECT_EQ(1, res.month);
        EXPECT_EQ(1, res.day);
        EXPECT_EQ(1, res.hour);
        EXPECT_EQ(59, res.minute);
        EXPECT_EQ(0, res.second);
    }
}

TEST_F(TimestampSIMDParsingTest, test_invalid_second_values) {
    date::ToDatetimeResult res;

    // Test second = 60 (invalid)
    {
        const char* str = "2024-01-01 01:30:60";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "second=60 should be invalid";
    }

    // Test second = 61 (invalid)
    {
        const char* str = "2024-01-01 01:30:61";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "second=61 should be invalid";
    }

    // Test second = 59 (valid)
    {
        const char* str = "2024-01-01 01:30:59";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_TRUE(is_valid) << "second=59 should be valid";
        EXPECT_FALSE(is_only_date) << "should include time component";
        EXPECT_EQ(2024, res.year);
        EXPECT_EQ(1, res.month);
        EXPECT_EQ(1, res.day);
        EXPECT_EQ(1, res.hour);
        EXPECT_EQ(30, res.minute);
        EXPECT_EQ(59, res.second);
    }
}

TEST_F(TimestampSIMDParsingTest, test_invalid_hour_values) {
    date::ToDatetimeResult res;

    // Test hour = 24 (invalid)
    {
        const char* str = "2024-01-01 24:00:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "hour=24 should be invalid";
    }

    // Test hour = 25 (invalid)
    {
        const char* str = "2024-01-01 25:00:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "hour=25 should be invalid";
    }

    // Test hour = 23 (valid)
    {
        const char* str = "2024-01-01 23:00:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_TRUE(is_valid) << "hour=23 should be valid";
        EXPECT_FALSE(is_only_date) << "should include time component";
        EXPECT_EQ(2024, res.year);
        EXPECT_EQ(1, res.month);
        EXPECT_EQ(1, res.day);
        EXPECT_EQ(23, res.hour);
        EXPECT_EQ(0, res.minute);
        EXPECT_EQ(0, res.second);
    }
}

TEST_F(TimestampSIMDParsingTest, test_edge_case_time_values) {
    date::ToDatetimeResult res;

    // Test all max valid values
    {
        const char* str = "2024-01-01 23:59:59";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_TRUE(is_valid) << "23:59:59 should be valid";
        EXPECT_FALSE(is_only_date);
        EXPECT_EQ(23, res.hour);
        EXPECT_EQ(59, res.minute);
        EXPECT_EQ(59, res.second);
    }

    // Test all min valid values
    {
        const char* str = "2024-01-01 00:00:00";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_TRUE(is_valid) << "00:00:00 should be valid";
        EXPECT_FALSE(is_only_date);
        EXPECT_EQ(0, res.hour);
        EXPECT_EQ(0, res.minute);
        EXPECT_EQ(0, res.second);
    }
}

TEST_F(TimestampSIMDParsingTest, test_valid_datetime_with_microseconds) {
    date::ToDatetimeResult res;

    // Test valid datetime with microseconds
    {
        const char* str = "2024-01-01 01:30:45.123456";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_TRUE(is_valid) << "datetime with microseconds should be valid";
        EXPECT_FALSE(is_only_date);
        EXPECT_EQ(2024, res.year);
        EXPECT_EQ(1, res.month);
        EXPECT_EQ(1, res.day);
        EXPECT_EQ(1, res.hour);
        EXPECT_EQ(30, res.minute);
        EXPECT_EQ(45, res.second);
        EXPECT_EQ(123456, res.microsecond);
    }

    // Test invalid time with microseconds (minute=61)
    {
        const char* str = "2024-01-01 01:61:45.123456";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "datetime with invalid minute should be invalid even with microseconds";
    }
}

TEST_F(TimestampSIMDParsingTest, test_iso8601_format_with_invalid_time) {
    date::ToDatetimeResult res;

    // Test ISO 8601 format with invalid minute
    {
        const char* str = "2024-01-01T01:61:00Z";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_FALSE(is_valid) << "ISO 8601 format with invalid minute should be invalid";
    }

    // Test ISO 8601 format with valid time
    {
        const char* str = "2024-01-01T01:30:00Z";
        auto [is_valid, is_only_date] = date::from_string_to_datetime(str, strlen(str), &res);
        EXPECT_TRUE(is_valid) << "ISO 8601 format with valid time should be valid";
        EXPECT_FALSE(is_only_date);
        EXPECT_EQ(1, res.hour);
        EXPECT_EQ(30, res.minute);
        EXPECT_EQ(0, res.second);
    }
}

} // namespace starrocks
