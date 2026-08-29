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

#include "util/trim.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

TEST(TrimSpacesTest, trim_spaces) {
    EXPECT_EQ("abc", trim_spaces("abc"));
    EXPECT_EQ("abc", trim_spaces("   abc"));
    EXPECT_EQ("abc", trim_spaces("abc   "));
    EXPECT_EQ("abc", trim_spaces("   abc   "));

    // Only the outermost spaces are removed.
    EXPECT_EQ("a b  c", trim_spaces("  a b  c  "));

    EXPECT_EQ("", trim_spaces(""));
    EXPECT_EQ("", trim_spaces(" "));
    EXPECT_EQ("", trim_spaces("      "));
}

TEST(TrimSpacesTest, keeps_the_other_whitespace) {
    // A tab, a newline or a carriage return is data for the CSV `trim_space` option and for URL
    // parsing, so it must survive.
    EXPECT_EQ("\tabc\t", trim_spaces("\tabc\t"));
    EXPECT_EQ("abc\n", trim_spaces(" abc\n "));
    EXPECT_EQ("abc\r", trim_spaces("abc\r"));
    EXPECT_EQ("\vabc\f", trim_spaces("  \vabc\f  "));
}

TEST(TrimSpacesTest, points_into_the_input_buffer) {
    const std::string input = "  abc  ";
    std::string_view trimmed = trim_spaces(input);
    EXPECT_EQ(input.data() + 2, trimmed.data());
    EXPECT_EQ(3, trimmed.size());

    // An all-space input still yields a view into the input rather than a null one.
    const std::string spaces = "   ";
    trimmed = trim_spaces(spaces);
    EXPECT_TRUE(trimmed.empty());
    EXPECT_NE(nullptr, trimmed.data());
}

TEST(TrimSpacesTest, handles_embedded_nul) {
    // The input is a view, not a C string, so a NUL is just another byte.
    const std::string input(" a\0b ", 5);
    EXPECT_EQ(std::string("a\0b", 3), std::string(trim_spaces(input)));
}

} // namespace starrocks
