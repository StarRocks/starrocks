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

#include "base/string/volnitsky.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

class VolnitskyTest : public ::testing::Test {};

TEST_F(VolnitskyTest, MainAlgorithmPathFindsNeedle) {
    const std::string needle = "pattern";
    std::string haystack(25000, 'x');
    const size_t expected_offset = 12345;
    haystack.replace(expected_offset, needle.size(), needle);

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    const char* result = searcher.search(haystack.data(), haystack.size());
    EXPECT_EQ(haystack.data() + expected_offset, result);
}

TEST_F(VolnitskyTest, FallbackPathWithShortNeedle) {
    const std::string haystack = "abcyd";
    const std::string needle = "y";

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    const char* result = searcher.search(haystack.data(), haystack.size());
    EXPECT_EQ(haystack.data() + 3, result);
}

TEST_F(VolnitskyTest, EmptyNeedleReturnsHaystackBegin) {
    const std::string haystack = "abcdef";
    VolnitskyUTF8 searcher("", 0, haystack.size());
    EXPECT_EQ(haystack.data(), searcher.search(haystack.data(), haystack.size()));
}

TEST_F(VolnitskyTest, NotFoundReturnsHaystackEnd) {
    const std::string haystack = "abcdef";
    const std::string needle = "xyz";

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    EXPECT_EQ(haystack.data() + haystack.size(), searcher.search(haystack.data(), haystack.size()));
}

TEST_F(VolnitskyTest, Utf8NeedleFoundAtExpectedOffset) {
    const std::string haystack =
            "StarRocks-"
            "\xE6\x95\xB0\xE6\x8D\xAE\xE5\xBA\x93"
            "-BE";
    const std::string needle = "\xE6\x95\xB0\xE6\x8D\xAE\xE5\xBA\x93";

    const size_t expected_offset = haystack.find(needle);
    ASSERT_NE(std::string::npos, expected_offset);

    VolnitskyUTF8 searcher(needle.data(), needle.size(), haystack.size());
    const char* result = searcher.search(haystack.data(), haystack.size());
    EXPECT_EQ(haystack.data() + expected_offset, result);
}

TEST_F(VolnitskyTest, BinaryNeedleWithEmbeddedNull) {
    const char haystack_raw[] = {'a', 'b', '\0', 'c', '\0', 'd', 'e', 'f'};
    const char needle_raw[] = {'c', '\0', 'd', 'e'};

    VolnitskyUTF8 searcher(needle_raw, sizeof(needle_raw), 0);
    const char* result = searcher.search(haystack_raw, sizeof(haystack_raw));
    EXPECT_EQ(haystack_raw + 3, result);
}

} // namespace starrocks
