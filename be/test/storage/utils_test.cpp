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

#include "storage/utils.h"

#include <gtest/gtest.h>

#include "runtime/mem_tracker.h"

namespace starrocks {
class TestUtils : public ::testing::Test {};
TEST_F(TestUtils, test_valid_decimal) {
    ASSERT_TRUE(valid_decimal("0", 0, 0));
    ASSERT_TRUE(valid_decimal("0", 2, 2));
    ASSERT_TRUE(valid_decimal("0", 10, 10));
    ASSERT_TRUE(valid_decimal("0.618", 10, 10));
    ASSERT_TRUE(valid_decimal("0.0618", 10, 10));
    ASSERT_TRUE(valid_decimal("0.0", 10, 10));
    ASSERT_TRUE(valid_decimal("-0.618", 10, 10));
    ASSERT_TRUE(valid_decimal("-0.0618", 10, 10));
    ASSERT_TRUE(valid_decimal("-0.0", 10, 10));
    ASSERT_FALSE(valid_decimal("3.14", 10, 10));
    ASSERT_FALSE(valid_decimal("31.4", 10, 10));
    ASSERT_FALSE(valid_decimal("314.15925", 10, 10));
    ASSERT_FALSE(valid_decimal("-3.14", 10, 10));
    ASSERT_FALSE(valid_decimal("-31.4", 10, 10));
    ASSERT_FALSE(valid_decimal("-314.15925", 10, 10));
    ASSERT_TRUE(valid_decimal("-3.14", 3, 2));
    ASSERT_TRUE(valid_decimal("-31.4", 3, 1));
    ASSERT_TRUE(valid_decimal("-314.15925", 8, 5));
    ASSERT_TRUE(valid_decimal("3.14", 3, 2));
    ASSERT_TRUE(valid_decimal("31.4", 3, 1));
    ASSERT_TRUE(valid_decimal("314.15925", 8, 5));
}

TEST_F(TestUtils, test_is_tracker_hit_hard_limit) {
    std::unique_ptr<MemTracker> tracker = std::make_unique<MemTracker>(1000, "test", nullptr);
    tracker->consume(2000);
    ASSERT_TRUE(is_tracker_hit_hard_limit(tracker.get(), 0.1));
    ASSERT_TRUE(is_tracker_hit_hard_limit(tracker.get(), 1.1));
    ASSERT_TRUE(is_tracker_hit_hard_limit(tracker.get(), 1.5));
    ASSERT_TRUE(is_tracker_hit_hard_limit(tracker.get(), 1.7));
    ASSERT_TRUE(!is_tracker_hit_hard_limit(tracker.get(), 2));
    ASSERT_TRUE(!is_tracker_hit_hard_limit(tracker.get(), 2.5));
    ASSERT_TRUE(!is_tracker_hit_hard_limit(tracker.get(), 3));
    ASSERT_TRUE(!is_tracker_hit_hard_limit(tracker.get(), 4));
}

TEST_F(TestUtils, test_valid_datetime) {
    ASSERT_TRUE(valid_datetime("2020-01-01 00:00:00"));
    ASSERT_TRUE(valid_datetime("2020-01-01 00:00:00.0"));
    ASSERT_TRUE(valid_datetime("2020-01-01 00:00:00.01"));
    ASSERT_TRUE(valid_datetime("2020-01-01 00:00:00.012345"));
    ASSERT_TRUE(valid_datetime("2020-01-01 00:00:00.1230"));
    ASSERT_FALSE(valid_datetime("2020-01-01 00:00:00.0123456"));
}

TEST_F(TestUtils, test_parse_data_size) {
    // Empty string should return 0
    ASSERT_EQ(0, parse_data_size(""));

    // String with only spaces should return 0
    ASSERT_EQ(0, parse_data_size("   "));

    // String with invalid unit should return 0
    ASSERT_EQ(0, parse_data_size("10Bytes"));
    ASSERT_EQ(0, parse_data_size("10X"));
    ASSERT_EQ(0, parse_data_size("abc"));
    ASSERT_EQ(0, parse_data_size("10.5XB"));

    // Only number, no unit, should be treated as bytes
    ASSERT_EQ(10, parse_data_size("10"));
    ASSERT_EQ(123, parse_data_size("123"));

    // Test with 'B' and spaces
    ASSERT_EQ(10, parse_data_size("10B"));
    ASSERT_EQ(10, parse_data_size("  10B  "));
    ASSERT_EQ(10, parse_data_size("10 b"));
    ASSERT_EQ(10, parse_data_size("10 b "));

    // Test with K/k/KB/kb
    ASSERT_EQ(10 * 1024, parse_data_size("10K"));
    ASSERT_EQ(10 * 1024, parse_data_size("10KB"));
    ASSERT_EQ(10 * 1024, parse_data_size("10k"));
    ASSERT_EQ(10 * 1024, parse_data_size("10kb"));
    ASSERT_EQ(10 * 1024, parse_data_size(" 10 KB "));

    // Test with M/m/MB/mb
    ASSERT_EQ(10 * 1024 * 1024, parse_data_size("10M"));
    ASSERT_EQ(10 * 1024 * 1024, parse_data_size("10MB"));
    ASSERT_EQ(10 * 1024 * 1024, parse_data_size("10m"));
    ASSERT_EQ(10 * 1024 * 1024, parse_data_size("10mb"));

    // Test with G/g/GB/gb
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, parse_data_size("10G"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, parse_data_size("10GB"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, parse_data_size("10g"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, parse_data_size("10gb"));

    // Test with T/t/TB/tb
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, parse_data_size("10T"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, parse_data_size("10TB"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, parse_data_size("10t"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, parse_data_size("10tb"));

    // Test with P/p/PB/pb
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, parse_data_size("10P"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, parse_data_size("10PB"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, parse_data_size("10p"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, parse_data_size("10pb"));

    // Test with decimal numbers
    ASSERT_EQ(1536, parse_data_size("1.5K"));
    ASSERT_EQ(1536, parse_data_size("1.5KB"));
    ASSERT_EQ(1572864, parse_data_size("1.5M"));
    ASSERT_EQ(1610612736, parse_data_size("1.5G"));
    ASSERT_EQ(0, parse_data_size("1.5X")); // invalid unit

    // Test with leading/trailing spaces
    ASSERT_EQ(10 * 1024, parse_data_size("   10K"));
    ASSERT_EQ(10 * 1024, parse_data_size("10K   "));
    ASSERT_EQ(10 * 1024, parse_data_size("   10K   "));

    // Test with spaces between number and unit
    ASSERT_EQ(10 * 1024, parse_data_size("10 K"));
    ASSERT_EQ(10 * 1024, parse_data_size("10   K"));
    ASSERT_EQ(10 * 1024, parse_data_size("  10   K  "));

    // Test with negative number (should return 0)
    ASSERT_EQ(0, parse_data_size("-10K"));

    // Test with value exceeding int64_t max (should return 0)
    ASSERT_EQ(0, parse_data_size("100000000000000000000P"));

    // Test with value below int64_t min (should return 0)
    ASSERT_EQ(0, parse_data_size("-100000000000000000000P"));
}

} // namespace starrocks
