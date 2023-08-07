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

#include "column/field.h"

#include <gtest/gtest.h>

#include "storage/aggregate_type.h"

namespace starrocks {

TEST(FieldTest, test_construct0) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", TYPE_INT, false);

    ASSERT_EQ(1, field1->id());
    ASSERT_EQ("c1", field1->name());
    ASSERT_EQ(TYPE_INT, field1->type()->type());
    ASSERT_FALSE(field1->is_nullable());
    ASSERT_FALSE(field1->is_key());
    ASSERT_EQ(STORAGE_AGGREGATE_NONE, field1->aggregate_method());
    ASSERT_EQ(0, field1->short_key_length());

    FieldPtr field2 = std::make_shared<Field>(2, "c2", TYPE_VARCHAR, true);

    ASSERT_EQ(2, field2->id());
    ASSERT_EQ("c2", field2->name());
    ASSERT_TRUE(field2->is_nullable());
    ASSERT_FALSE(field2->is_key());
    ASSERT_EQ(TYPE_VARCHAR, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_NONE, field2->aggregate_method());
    ASSERT_EQ(0, field2->short_key_length());
}

TEST(FieldTest, test_construct1) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, false);

    ASSERT_EQ(1, field1->id());
    ASSERT_EQ("c1", field1->name());
    ASSERT_TRUE(field1->is_key());
    ASSERT_FALSE(field1->is_nullable());
    ASSERT_EQ(TYPE_INT, field1->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field1->aggregate_method());
    ASSERT_EQ(10, field1->short_key_length());

    FieldPtr field2 =
            std::make_shared<Field>(2, "c2", get_type_info(TYPE_VARCHAR), STORAGE_AGGREGATE_MIN, 12, true, false);

    ASSERT_EQ(2, field2->id());
    ASSERT_EQ("c2", field2->name());
    ASSERT_EQ(TYPE_VARCHAR, field2->type()->type());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_VARCHAR, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MIN, field2->aggregate_method());
    ASSERT_EQ(12, field2->short_key_length());
}

TEST(FieldTest, test_copy_ctor) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, false);
    FieldPtr field2 = std::make_shared<Field>(*field1);

    ASSERT_EQ(1, field2->id());
    ASSERT_EQ("c1", field2->name());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_INT, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field2->aggregate_method());
    ASSERT_EQ(10, field2->short_key_length());
}

TEST(FieldTest, test_move_ctor) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, false);
    FieldPtr field2 = std::make_shared<Field>(std::move(*field1));

    ASSERT_EQ(1, field2->id());
    ASSERT_EQ("c1", field2->name());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_INT, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field2->aggregate_method());
    ASSERT_EQ(10, field2->short_key_length());
}

TEST(FieldTest, test_copy_assign) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, false);
    FieldPtr field2 =
            std::make_shared<Field>(2, "c2", get_type_info(TYPE_VARCHAR), STORAGE_AGGREGATE_MIN, 100, false, true);
    *field2 = *field1;

    ASSERT_EQ(1, field2->id());
    ASSERT_EQ("c1", field2->name());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_INT, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field2->aggregate_method());
    ASSERT_EQ(10, field2->short_key_length());
}

TEST(FieldTest, test_move_assign) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, false);
    FieldPtr field2 =
            std::make_shared<Field>(2, "c2", get_type_info(TYPE_VARCHAR), STORAGE_AGGREGATE_MIN, 100, false, true);
    *field2 = std::move(*field1);

    ASSERT_EQ(1, field2->id());
    ASSERT_EQ("c1", field2->name());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_INT, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field2->aggregate_method());
    ASSERT_EQ(10, field2->short_key_length());
}

TEST(FieldTest, test_with_type) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, false);
    FieldPtr field2 = field1->with_type(get_type_info(TYPE_VARCHAR));

    ASSERT_EQ(1, field2->id());
    ASSERT_EQ("c1", field2->name());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_VARCHAR, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field2->aggregate_method());
    ASSERT_EQ(10, field2->short_key_length());
}

TEST(FieldTest, test_with_name) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, false);
    FieldPtr field2 = field1->with_name("c2");

    ASSERT_EQ(1, field2->id());
    ASSERT_EQ("c2", field2->name());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_INT, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field2->aggregate_method());
    ASSERT_EQ(10, field2->short_key_length());
}

TEST(FieldTest, test_with_nullable) {
    FieldPtr field1 = std::make_shared<Field>(1, "c1", get_type_info(TYPE_INT), STORAGE_AGGREGATE_MAX, 10, true, true);
    FieldPtr field2 = field1->with_nullable(false);

    ASSERT_EQ(1, field2->id());
    ASSERT_EQ("c1", field2->name());
    ASSERT_TRUE(field2->is_key());
    ASSERT_FALSE(field2->is_nullable());
    ASSERT_EQ(TYPE_INT, field2->type()->type());
    ASSERT_EQ(STORAGE_AGGREGATE_MAX, field2->aggregate_method());
    ASSERT_EQ(10, field2->short_key_length());
}

} // namespace starrocks
