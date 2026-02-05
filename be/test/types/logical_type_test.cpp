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

#include "types/logical_type.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(LogicalTypeTest, StringToLogicalType) {
    EXPECT_EQ(TYPE_INT, string_to_logical_type("INT"));
    EXPECT_EQ(TYPE_INT, string_to_logical_type("int"));
    EXPECT_EQ(TYPE_DATE, string_to_logical_type("DATE"));
    EXPECT_EQ(TYPE_DATE, string_to_logical_type("DATE_V2"));
    EXPECT_EQ(TYPE_VARBINARY, string_to_logical_type("VARBINARY"));
    EXPECT_EQ(TYPE_UNKNOWN, string_to_logical_type("not_a_type"));
}

TEST(LogicalTypeTest, LogicalTypeToString) {
    EXPECT_STREQ("INT", logical_type_to_string(TYPE_INT));
    EXPECT_STREQ("DATE_V2", logical_type_to_string(TYPE_DATE));
    EXPECT_STREQ("TIMESTAMP", logical_type_to_string(TYPE_DATETIME));
    EXPECT_STREQ("UNKNOWN", logical_type_to_string(TYPE_UNKNOWN));
}

TEST(LogicalTypeTest, TypePredicates) {
    EXPECT_TRUE(is_integer_type(TYPE_TINYINT));
    EXPECT_TRUE(is_integer_type(TYPE_BIGINT));
    EXPECT_FALSE(is_integer_type(TYPE_DOUBLE));

    EXPECT_TRUE(is_string_type(TYPE_CHAR));
    EXPECT_TRUE(is_string_type(TYPE_VARCHAR));
    EXPECT_FALSE(is_string_type(TYPE_BINARY));

    EXPECT_TRUE(is_binary_type(TYPE_BINARY));
    EXPECT_TRUE(is_binary_type(TYPE_VARBINARY));
    EXPECT_FALSE(is_binary_type(TYPE_VARCHAR));
}

} // namespace starrocks
