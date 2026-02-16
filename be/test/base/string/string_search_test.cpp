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

#include "base/string/string_search.hpp"

#include <gtest/gtest.h>

namespace starrocks {

class StringSearchTest : public ::testing::Test {};

TEST_F(StringSearchTest, FindAndNotFound) {
    Slice pattern("needle");
    StringSearch searcher(&pattern);

    EXPECT_EQ(4, searcher.search(Slice("abc needle xyz")));
    EXPECT_EQ(-1, searcher.search(Slice("abc need xyz")));
}

TEST_F(StringSearchTest, SingleCharacterPattern) {
    Slice pattern("x");
    StringSearch searcher(&pattern);

    EXPECT_EQ(3, searcher.search(Slice("abcxdef")));
    EXPECT_EQ(-1, searcher.search(Slice("abcdef")));
}

TEST_F(StringSearchTest, EmptyPatternReturnsNotFound) {
    Slice empty_pattern("");
    StringSearch searcher(&empty_pattern);
    EXPECT_EQ(-1, searcher.search(Slice("abcdef")));

    StringSearch default_searcher;
    EXPECT_EQ(-1, default_searcher.search(Slice("abcdef")));
}

TEST_F(StringSearchTest, BinaryHaystackWithEmbeddedNull) {
    const char haystack_raw[] = {'a', 'b', '\0', 'c', '\0', 'd', 'e'};
    const char needle_raw[] = {'c', '\0', 'd'};

    Slice pattern(needle_raw, sizeof(needle_raw));
    Slice haystack(haystack_raw, sizeof(haystack_raw));
    StringSearch searcher(&pattern);

    EXPECT_EQ(3, searcher.search(haystack));
}

} // namespace starrocks
