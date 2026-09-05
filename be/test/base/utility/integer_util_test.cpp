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

#include "base/utility/integer_util.h"

#include <gtest/gtest.h>

#include "base/types/int256.h"

namespace starrocks {

TEST(IntegerUtilTest, IntegerToStringHandlesLargeValues) {
    const int256_t value = INT256_MAX;
    const std::string expected = std::to_string(value);
    ASSERT_GT(expected.size(), 64u);
    EXPECT_EQ(integer_to_string(value), expected);
}

} // namespace starrocks
