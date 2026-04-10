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

#include "base/string/slice.h"

#include <gtest/gtest.h>

namespace starrocks {

class SliceTest : public testing::Test {};

TEST_F(SliceTest, testFindChar) {
    std::string text("ABCABABCDABCDABD");
    const Slice slice(text);

    int64_t last_match = 0, cur_match = 0;
    while (true) {
        cur_match = text.find('A', last_match);
        EXPECT_EQ(cur_match, slice.find('A', last_match));

        if (cur_match == -1) break;
        last_match = cur_match + 1;
    }
}

TEST_F(SliceTest, testFindCharEdgeCases) {
    // Empty slice
    std::string empty_text("");
    Slice empty_slice(empty_text);
    EXPECT_EQ(-1, empty_slice.find('A'));

    // Single character slice - found
    std::string single_char_text("A");
    Slice single_char_found(single_char_text);
    EXPECT_EQ(0, single_char_found.find('A'));

    // Single character slice - not found
    std::string single_char_not_found_text("B");
    Slice single_char_not_found(single_char_not_found_text);
    EXPECT_EQ(-1, single_char_not_found.find('A'));

    // Find with offset
    std::string muilti_char_text("AAABBBAAA");
    Slice multi_char(muilti_char_text);
    EXPECT_EQ(0, multi_char.find('A', 0));   // First A
    EXPECT_EQ(1, multi_char.find('A', 1));   // Second A
    EXPECT_EQ(2, multi_char.find('A', 2));   // Third A
    EXPECT_EQ(3, multi_char.find('B', 3));   // First B
    EXPECT_EQ(-1, multi_char.find('A', 10)); // Offset beyond size
    EXPECT_EQ(-1, multi_char.find('X'));     // Character not present

    // Negative offset
    EXPECT_EQ(-1, multi_char.find('A', -1)); // Should start from 0
}

TEST_F(SliceTest, testBuildNext) {
    // Simple case
    std::string pattern1_text("ABABC");
    Slice pattern1(pattern1_text);
    auto next1 = pattern1.build_next();
    std::vector<size_t> expected1 = {0, 0, 1, 2, 0};
    EXPECT_EQ(expected1, next1);

    // All same characters
    std::string pattern2_text("AAAA");
    Slice pattern2(pattern2_text);
    auto next2 = pattern2.build_next();
    std::vector<size_t> expected2 = {0, 1, 2, 3};
    EXPECT_EQ(expected2, next2);

    // No repetition
    std::string pattern3_text("ABCD");
    Slice pattern3(pattern3_text);
    auto next3 = pattern3.build_next();
    std::vector<size_t> expected3 = {0, 0, 0, 0};
    EXPECT_EQ(expected3, next3);

    // Empty slice
    Slice pattern4("");
    auto next4 = pattern4.build_next();
    EXPECT_TRUE(next4.empty());
}

TEST_F(SliceTest, testFindSlice) {
    std::string s_text("ABC ABCDAB ABCDABCDABDE");
    Slice text(s_text);
    std::string s_pattern("ABCDABD");
    Slice pattern(s_pattern);
    auto next = pattern.build_next();
    std::vector<size_t> expected = {0, 0, 0, 0, 1, 2, 0};
    EXPECT_EQ(7, next.size());
    EXPECT_EQ(expected, next);

    // Find pattern in text
    int64_t pos = text.find(pattern, next);
    EXPECT_EQ(15, pos);

    // Find with offset before match
    pos = text.find(pattern, next, 0);
    EXPECT_EQ(15, pos);

    // Find with offset after match
    pos = text.find(pattern, next, 16);
    EXPECT_EQ(-1, pos);
}

TEST_F(SliceTest, testFindSliceEdgeCases) {
    // Empty pattern
    std::string stext1("ABC");
    Slice text1(stext1);
    std::string spattern1("");
    Slice empty_pattern(spattern1);
    auto next_empty = empty_pattern.build_next();
    EXPECT_EQ(-1, text1.find(empty_pattern, next_empty));

    // Pattern larger than text
    std::string stext("ABC");
    Slice small_text(stext);
    std::string slarge_pattern("ABCDEFGH");
    Slice large_pattern(slarge_pattern);
    auto next_large = large_pattern.build_next();
    EXPECT_EQ(-1, small_text.find(large_pattern, next_large));

    // Exact match
    std::string s_exact_text("ABCDEF");
    Slice exact_text(s_exact_text);
    std::string s_exact_pattern("ABCDEF");
    Slice exact_pattern(s_exact_pattern);
    auto next_exact = exact_pattern.build_next();
    EXPECT_EQ(0, exact_text.find(exact_pattern, next_exact));

    // No match
    std::string s_text2("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    Slice text2(s_text2);
    std::string s_pattern2("XYZABC");
    Slice pattern2(s_pattern2);
    auto next2 = pattern2.build_next();
    EXPECT_EQ(-1, text2.find(pattern2, next2));

    // Match at the end
    std::string s_text3("ABCDEFGHIJKLMNOP");
    Slice text3(s_text3);
    std::string s_pattern3("MNOP");
    Slice pattern3(s_pattern3);
    auto next3 = pattern3.build_next();
    EXPECT_EQ(12, text3.find(pattern3, next3));

    // Repeated pattern
    std::string s_text4("ABABABAB");
    Slice text4(s_text4);
    std::string s_pattern4("ABA");
    Slice pattern4(s_pattern4);
    auto next4 = pattern4.build_next();
    EXPECT_EQ(0, text4.find(pattern4, next4, 0));  // First occurrence
    EXPECT_EQ(2, text4.find(pattern4, next4, 1));  // Second occurrence
    EXPECT_EQ(4, text4.find(pattern4, next4, 3));  // Third occurrence
    EXPECT_EQ(-1, text4.find(pattern4, next4, 5)); // Fourth occurrence

    // Invalid offset
    std::string s_text5("ABCDEF");
    Slice text5(s_text5);
    std::string s_pattern5("CDE");
    Slice pattern5(s_pattern5);
    auto next5 = pattern5.build_next();
    EXPECT_EQ(-1, text5.find(pattern5, next5, -1));  // Negative offset
    EXPECT_EQ(-1, text5.find(pattern5, next5, 100)); // Offset too large

    // Invalid next array size
    std::string s_text6("ABCDEF");
    Slice text6(s_text6);
    std::string s_pattern6("ABC");
    Slice pattern6(s_pattern6);
    std::vector<size_t> invalid_next = {0, 1}; // Wrong size
    EXPECT_EQ(-1, text6.find(pattern6, invalid_next));

    // Empty text
    std::string s_text7("");
    Slice empty_text(s_text7);
    std::string s_pattern7("A");
    Slice pattern7(s_pattern7);
    auto next7 = pattern7.build_next();
    EXPECT_EQ(-1, empty_text.find(pattern7, next7));
}

TEST_F(SliceTest, testFindSliceAdditionalCases) {
    // Single character pattern
    std::string s_text1("ABCDEFGHIJKLMNO");
    Slice text1(s_text1);
    std::string s_pattern1("G");
    Slice pattern1(s_pattern1);
    auto next1 = pattern1.build_next();
    EXPECT_EQ(6, text1.find(pattern1, next1));

    // Pattern at the beginning
    std::string s_text2("ABCDEF");
    Slice text2(s_text2);
    std::string s_pattern2("ABC");
    Slice pattern2(s_pattern2);
    auto next2 = pattern2.build_next();
    EXPECT_EQ(0, text2.find(pattern2, next2));

    // Pattern at the end
    std::string s_text3("ABCDEF");
    Slice text3(s_text3);
    std::string s_pattern3("DEF");
    Slice pattern3(s_pattern3);
    auto next3 = pattern3.build_next();
    EXPECT_EQ(3, text3.find(pattern3, next3));

    // All same characters in pattern and text
    std::string s_text4("AAAAAA");
    Slice text4(s_text4);
    std::string s_pattern4("AAA");
    Slice pattern4(s_pattern4);
    auto next4 = pattern4.build_next();
    EXPECT_EQ(0, text4.find(pattern4, next4));

    // Complex KMP backtracking case
    std::string s_text5("ABABACA");
    Slice text5(s_text5);
    std::string s_pattern5("ABABAC");
    Slice pattern5(s_pattern5);
    auto next5 = pattern5.build_next();
    std::vector<size_t> expected_next5 = {0, 0, 1, 2, 3, 0};
    EXPECT_EQ(expected_next5, next5);
    EXPECT_EQ(0, text5.find(pattern5, next5));

    // Partial match that fails
    std::string s_text6("ABABACA");
    Slice text6(s_text6);
    std::string s_pattern6("ABABAX");
    Slice pattern6(s_pattern6);
    auto next6 = pattern6.build_next();
    EXPECT_EQ(-1, text6.find(pattern6, next6));
}

} // namespace starrocks
