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

#include "util/url_coding.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

TEST(UrlCodingTest, UrlDecodeBasic) {
    // Simple decode
    auto res = url_decode("abc");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "abc");
    // Encoded percent
    res = url_decode("a%20b%21c");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "a b!c");
    res = url_decode("testStreamLoad%E6%A1%8C");
    EXPECT_TRUE(res.ok());
    const char c1[32] = "testStreamLoad桌";
    int ret = memcmp(c1, res.value().c_str(), 17);
    EXPECT_EQ(ret, 0);
}

TEST(UrlCodingTest, UrlDecodeEdgeCases) {
    // Empty string
    auto res = url_decode("");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "");
    // + should not be decoded to space
    res = url_decode("a+b");
    EXPECT_TRUE(res.ok());
    EXPECT_EQ(res.value(), "a+b");
}

} // namespace starrocks
