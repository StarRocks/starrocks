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

#include "storage/utils.h"

#include <gtest/gtest.h>

namespace starrocks {
class TestUtils : public ::testing::Test {};
TEST_F(TestUtils, test_valid_decimal) {
    ASSERT_TRUE(valid_decimal("0", 0, 0));
    ASSERT_TRUE(valid_decimal("0", 2, 2));
    ASSERT_TRUE(valid_decimal("0", 10, 10));
    ASSERT_TRUE(valid_decimal("0.618", 10, 10));
    ASSERT_TRUE(valid_decimal("0.0618", 10, 10));
    ASSERT_TRUE(valid_decimal("0.0", 10, 10));
    ASSERT_TRUE(valid_decimal("-0.618", 10, 10));
    ASSERT_TRUE(valid_decimal("-0.0618", 10, 10));
    ASSERT_TRUE(valid_decimal("-0.0", 10, 10));
    ASSERT_FALSE(valid_decimal("3.14", 10, 10));
    ASSERT_FALSE(valid_decimal("31.4", 10, 10));
    ASSERT_FALSE(valid_decimal("314.15925", 10, 10));
    ASSERT_FALSE(valid_decimal("-3.14", 10, 10));
    ASSERT_FALSE(valid_decimal("-31.4", 10, 10));
    ASSERT_FALSE(valid_decimal("-314.15925", 10, 10));
    ASSERT_TRUE(valid_decimal("-3.14", 3, 2));
    ASSERT_TRUE(valid_decimal("-31.4", 3, 1));
    ASSERT_TRUE(valid_decimal("-314.15925", 8, 5));
    ASSERT_TRUE(valid_decimal("3.14", 3, 2));
    ASSERT_TRUE(valid_decimal("31.4", 3, 1));
    ASSERT_TRUE(valid_decimal("314.15925", 8, 5));
}
} // namespace starrocks
