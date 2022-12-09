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

#include "util/c_string.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(CStringTest, test_all) {
    CString s1;
    CString s2;
    EXPECT_TRUE(s1.empty());
    EXPECT_EQ(0, s1.size());
    EXPECT_TRUE(s1.data() != nullptr);
    EXPECT_EQ(s1, s2);
    EXPECT_LE(s1, s2);
    EXPECT_GE(s1, s2);
    EXPECT_FALSE(s1 != s2);
    EXPECT_FALSE(s1 > s2);
    EXPECT_FALSE(s1 < s2);

    s2.assign("", 0);
    EXPECT_EQ(s1, s2);
    EXPECT_EQ(0, s2.size());
    EXPECT_TRUE(s2.empty());
    EXPECT_EQ(s1, s2);
    EXPECT_LE(s1, s2);
    EXPECT_GE(s1, s2);
    EXPECT_FALSE(s1 != s2);
    EXPECT_FALSE(s1 > s2);
    EXPECT_FALSE(s1 < s2);

    s2.assign("string 1", 8);
    EXPECT_FALSE(s2.empty());
    EXPECT_EQ("string 1", std::string_view(s2.data(), s2.size()));
    EXPECT_FALSE(s1 == s2);
    EXPECT_TRUE(s1 != s2);
    EXPECT_TRUE(s1 < s2);
    EXPECT_TRUE(s1 <= s2);
    EXPECT_FALSE(s1 > s2);
    EXPECT_FALSE(s1 >= s2);

    // copy ctor/copy assign
    CString s3(s2);
    EXPECT_NE(s2.data(), s3.data());
    EXPECT_EQ("string 1", std::string_view(s3.data(), s3.size()));
    EXPECT_EQ(s2, s3);

    CString s4;
    s4 = s2;
    EXPECT_NE(s2.data(), s4.data());
    EXPECT_EQ("string 1", std::string_view(s4.data(), s4.size()));
    EXPECT_EQ(s2, s4);
    EXPECT_EQ(s3, s4);

    // move ctor/move assign
    CString s5(std::move(s3));
    EXPECT_EQ(0, s3.size());
    EXPECT_TRUE(s3.empty());
    EXPECT_TRUE(s3.data() != nullptr);
    EXPECT_EQ("string 1", std::string_view(s5.data(), s5.size()));

    CString s6;
    s6 = std::move(s5);
    EXPECT_EQ(0, s5.size());
    EXPECT_TRUE(s5.empty());
    EXPECT_TRUE(s5.data() != nullptr);
    EXPECT_EQ("string 1", std::string_view(s6.data(), s6.size()));
}

} // namespace starrocks
