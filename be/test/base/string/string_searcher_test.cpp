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

#include "base/string/string_searcher.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

class StringSearcherTest : public ::testing::Test {};

TEST_F(StringSearcherTest, FindAndNotFound) {
    const std::string haystack = "abchellodef";
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(haystack.data());
    const uint8_t* end = begin + haystack.size();

    const char needle[] = "hello";
    StringSearcher searcher(needle, 5);
    EXPECT_EQ(begin + 3, searcher.search(begin, end));

    const char missing_needle[] = "xyz";
    StringSearcher missing(missing_needle, 3);
    EXPECT_EQ(end, missing.search(begin, end));
}

TEST_F(StringSearcherTest, EmptyNeedleReturnsHaystackBegin) {
    const std::string haystack = "abcdef";
    const uint8_t* begin = reinterpret_cast<const uint8_t*>(haystack.data());
    const uint8_t* end = begin + haystack.size();

    const char needle[] = "";
    StringSearcher searcher(needle, 0);
    EXPECT_EQ(begin, searcher.search(begin, end));
}

TEST_F(StringSearcherTest, EmbeddedNullNeedle) {
    const uint8_t haystack_raw[] = {'a', 'b', 'c', '\0', 'd', 'e'};
    const uint8_t needle_raw[] = {'c', '\0', 'd'};
    const uint8_t* begin = haystack_raw;
    const uint8_t* end = begin + sizeof(haystack_raw);

    StringSearcher searcher(needle_raw, sizeof(needle_raw));
    EXPECT_EQ(begin + 2, searcher.search(begin, end));
}

TEST_F(StringSearcherTest, LibcSearcherFindAndMiss) {
    const char haystack[] = "aaaaab";
    LibcASCIICaseSensitiveStringSearcher searcher("aab", 3);
    EXPECT_EQ(haystack + 3, searcher.search(haystack, 6));

    LibcASCIICaseSensitiveStringSearcher missing("AAB", 3);
    EXPECT_EQ(nullptr, missing.search(haystack, 6));
}

} // namespace starrocks
