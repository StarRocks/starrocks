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

#include "types/type_checker_manager.h"
#include <gtest/gtest.h>



namespace starrocks {


class TypeCheckerTest : public ::testing::Test {

protected:
    TypeCheckerManager&  type_checker_manager_ = TypeCheckerManager::getInstance();

};

TEST_F(TypeCheckerTest, SupportByteType) {
    SlotDescriptor boolean_type_slot(0, "boolean_type_slot", TypeDescriptor(TYPE_BOOLEAN));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Byte", &boolean_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_BOOLEAN);

    SlotDescriptor tinyint_type_slot(0, "tinyint_type_slot", TypeDescriptor(TYPE_TINYINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Byte", &tinyint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_TINYINT);

    SlotDescriptor smallint_type_slot(0, "smallint_type_slot", TypeDescriptor(TYPE_SMALLINT));
    status_or_type = type_checker_manager_.checkType("java.lang.Byte", &smallint_type_slot);
    ASSERT_TRUE(status_or_type.ok());
    ASSERT_EQ(status_or_type.value(), LogicalType::TYPE_SMALLINT);

}


TEST_F(TypeCheckerTest, NotSupportByteType) {
    SlotDescriptor unknown_type_slot(0, "unknown_type_slot", TypeDescriptor(TYPE_UNKNOWN));
    auto status_or_type = type_checker_manager_.checkType("java.lang.Byte", &unknown_type_slot);
    ASSERT_FALSE(status_or_type.ok());
}


} // namespace starrocks
