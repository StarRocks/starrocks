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

#include "util/utf8_encoding.h"

#include <gtest/gtest.h>

#include <string_view>
#include <vector>

namespace starrocks {

static std::string_view encoded_value_as_string_view(const EncodedUtf8Char& value) {
    Slice slice = value;
    return {slice.data, slice.size};
}

TEST(Utf8EncodingTest, clampsTruncatedTailLeadByteToInput) {
    const char input[] = {'a', static_cast<char>(0xF0)};
    std::vector<EncodedUtf8Char> values;

    EXPECT_EQ(2, encode_utf8_chars(Slice(input, sizeof(input)), &values));
    ASSERT_EQ(2, values.size());

    EXPECT_EQ(std::string_view(input, 1), encoded_value_as_string_view(values[0]));
    EXPECT_EQ(std::string_view(input + 1, 1), encoded_value_as_string_view(values[1]));
}

} // namespace starrocks
