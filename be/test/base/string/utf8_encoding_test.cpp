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

#include "base/string/utf8_encoding.h"

#include <gtest/gtest.h>

#include <string_view>
#include <unordered_set>
#include <vector>

namespace starrocks {

static std::string_view encoded_value_as_string_view(const EncodedUtf8Char& value) {
    Slice slice = value;
    return {slice.data, slice.size};
}

TEST(Utf8EncodingTest, encodesAsciiAndMultibyteCharacters) {
    const char input[] = {'a',
                          static_cast<char>(0xE8),
                          static_cast<char>(0x86),
                          static_cast<char>(0xA8),
                          static_cast<char>(0xF0),
                          static_cast<char>(0x9F),
                          static_cast<char>(0x98),
                          static_cast<char>(0x84)};
    std::vector<EncodedUtf8Char> values;

    EXPECT_EQ(3, encode_utf8_chars(Slice(input, sizeof(input)), &values));
    ASSERT_EQ(3, values.size());

    EXPECT_EQ(std::string_view(input, 1), encoded_value_as_string_view(values[0]));
    EXPECT_EQ(std::string_view(input + 1, 3), encoded_value_as_string_view(values[1]));
    EXPECT_EQ(std::string_view(input + 4, 4), encoded_value_as_string_view(values[2]));
}

TEST(Utf8EncodingTest, defaultValueConvertsToEmptySlice) {
    EncodedUtf8Char value;
    Slice slice = value;

    EXPECT_TRUE(value.is_empty());
    EXPECT_EQ(0, slice.size);
}

TEST(Utf8EncodingTest, equalityAndHashUseEncodedBytes) {
    const char one[] = {'x'};
    const char two[] = {'y'};
    EncodedUtf8Char first(one, sizeof(one));
    EncodedUtf8Char first_copy(one, sizeof(one));
    EncodedUtf8Char second(two, sizeof(two));

    EXPECT_EQ(first, first_copy);
    EXPECT_FALSE(first == second);

    std::unordered_set<EncodedUtf8Char, EncodedUtf8CharHash> values;
    values.insert(first);
    values.insert(first_copy);
    values.insert(second);
    EXPECT_EQ(2, values.size());
}

} // namespace starrocks
