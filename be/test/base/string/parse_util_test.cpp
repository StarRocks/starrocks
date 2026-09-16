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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/parse_util_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "base/string/parse_util.h"

#include <gtest/gtest.h>

#include <string>

#include "base/testutil/assert.h"

namespace starrocks {

const static int64_t test_memory_limit = 10000;

static void test_parse_mem_spec(const std::string& mem_spec_str, int64_t result) {
    ASSIGN_OR_ASSERT_FAIL(int64_t bytes, ParseUtil::parse_mem_spec(mem_spec_str, test_memory_limit));
    ASSERT_EQ(result, bytes);
}

TEST(TestParseMemSpec, Normal) {
    test_parse_mem_spec("1", 1);

    test_parse_mem_spec("100b", 100);
    test_parse_mem_spec("100B", 100);

    test_parse_mem_spec("5k", 5 * 1024L);
    test_parse_mem_spec("512K", 512 * 1024L);

    test_parse_mem_spec("4m", 4 * 1024 * 1024L);
    test_parse_mem_spec("46M", 46 * 1024 * 1024L);

    test_parse_mem_spec("8g", 8 * 1024 * 1024 * 1024L);
    test_parse_mem_spec("128G", 128 * 1024 * 1024 * 1024L);

    test_parse_mem_spec("8t", 8L * 1024 * 1024 * 1024 * 1024L);
    test_parse_mem_spec("128T", 128L * 1024 * 1024 * 1024 * 1024L);

    test_parse_mem_spec("20%", test_memory_limit * 0.2);
    test_parse_mem_spec("-1", -1);
    test_parse_mem_spec("", 0);
}

TEST(TestParseMemSpec, Bad) {
    std::vector<std::string> bad_values{{"1gib"}, {"1%b"}, {"1b%"}, {"gb"},  {"1GMb"}, {"1b1Mb"}, {"1kib"},
                                        {"1Bb"},  {"1%%"}, {"1.1"}, {"1pb"}, {"1eb"},  {"%"}};
    for (const auto& value : bad_values) {
        ASSERT_ERROR(ParseUtil::parse_mem_spec(value, test_memory_limit));
    }
}

TEST(TestParseDataSize, Compatibility) {
    // Empty string should return 0
    ASSERT_EQ(0, ParseUtil::parse_data_size(""));

    // String with only spaces should return 0
    ASSERT_EQ(0, ParseUtil::parse_data_size("   "));

    // String with invalid unit should return 0
    ASSERT_EQ(0, ParseUtil::parse_data_size("10Bytes"));
    ASSERT_EQ(0, ParseUtil::parse_data_size("10X"));
    ASSERT_EQ(0, ParseUtil::parse_data_size("abc"));
    ASSERT_EQ(0, ParseUtil::parse_data_size("10.5XB"));

    // Only number, no unit, should be treated as bytes
    ASSERT_EQ(10, ParseUtil::parse_data_size("10"));
    ASSERT_EQ(123, ParseUtil::parse_data_size("123"));

    // Test with 'B' and spaces
    ASSERT_EQ(10, ParseUtil::parse_data_size("10B"));
    ASSERT_EQ(10, ParseUtil::parse_data_size("  10B  "));
    ASSERT_EQ(10, ParseUtil::parse_data_size("10 b"));
    ASSERT_EQ(10, ParseUtil::parse_data_size("10 b "));

    // Test with K/k/KB/kb
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("10K"));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("10KB"));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("10k"));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("10kb"));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size(" 10 KB "));

    // Test with M/m/MB/mb
    ASSERT_EQ(10 * 1024 * 1024, ParseUtil::parse_data_size("10M"));
    ASSERT_EQ(10 * 1024 * 1024, ParseUtil::parse_data_size("10MB"));
    ASSERT_EQ(10 * 1024 * 1024, ParseUtil::parse_data_size("10m"));
    ASSERT_EQ(10 * 1024 * 1024, ParseUtil::parse_data_size("10mb"));

    // Test with G/g/GB/gb
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10G"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10GB"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10g"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10gb"));

    // Test with T/t/TB/tb
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10T"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10TB"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10t"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10tb"));

    // Test with P/p/PB/pb
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10P"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10PB"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10p"));
    ASSERT_EQ(10LL * 1024 * 1024 * 1024 * 1024 * 1024, ParseUtil::parse_data_size("10pb"));

    // Test with decimal numbers
    ASSERT_EQ(1536, ParseUtil::parse_data_size("1.5K"));
    ASSERT_EQ(1536, ParseUtil::parse_data_size("1.5KB"));
    ASSERT_EQ(1572864, ParseUtil::parse_data_size("1.5M"));
    ASSERT_EQ(1610612736, ParseUtil::parse_data_size("1.5G"));
    ASSERT_EQ(0, ParseUtil::parse_data_size("1.5X")); // invalid unit

    // Test with leading/trailing spaces
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("   10K"));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("10K   "));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("   10K   "));

    // Test with spaces between number and unit
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("10 K"));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("10   K"));
    ASSERT_EQ(10 * 1024, ParseUtil::parse_data_size("  10   K  "));

    // Test with negative number (should return 0)
    ASSERT_EQ(0, ParseUtil::parse_data_size("-10K"));

    // Test with value exceeding int64_t max (should return 0)
    ASSERT_EQ(0, ParseUtil::parse_data_size("100000000000000000000P"));

    // Test with value below int64_t min (should return 0)
    ASSERT_EQ(0, ParseUtil::parse_data_size("-100000000000000000000P"));
}

} // namespace starrocks
