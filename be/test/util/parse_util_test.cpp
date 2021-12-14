// This file is made available under Elastic License 2.0.
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

#include "util/parse_util.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

static void test_parse_mem_spec(const std::string& mem_spec_str, int64_t result) {
    int64_t bytes = ParseUtil::parse_mem_spec(mem_spec_str);
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

    int64_t bytes = ParseUtil::parse_mem_spec("20%");
    ASSERT_GT(bytes, 0);
}

TEST(TestParseMemSpec, Bad) {
    std::vector<std::string> bad_values;
    bad_values.push_back("1gib");
    bad_values.push_back("1%b");
    bad_values.push_back("1b%");
    bad_values.push_back("gb");
    bad_values.push_back("1GMb");
    bad_values.push_back("1b1Mb");
    bad_values.push_back("1kib");
    bad_values.push_back("1Bb");
    bad_values.push_back("1%%");
    bad_values.push_back("1.1");
    bad_values.push_back("1pb");
    bad_values.push_back("1eb");
    bad_values.push_back("%");
    for (const auto& value : bad_values) {
        int64_t bytes = ParseUtil::parse_mem_spec(value);
        ASSERT_EQ(-1, bytes);
    }
}

} // namespace starrocks
