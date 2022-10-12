// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
