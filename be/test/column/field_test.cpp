// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/column/field_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARjjjjjjjjjkjRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "column/field.h"

#include <gtest/gtest.h>

namespace starrocks {
namespace vectorized {

TEST(FieldTest, test_construct) {
    FieldPtr field1 = std::make_shared<Field>(1, std::string("c1"), get_type_info(OLAP_FIELD_TYPE_INT), false);

    ASSERT_EQ("c1", field1->name());
    ASSERT_FALSE(field1->is_nullable());

    FieldPtr field2 =
            std::make_shared<Field>(2, std::move(std::string("c2")), get_type_info(OLAP_FIELD_TYPE_VARCHAR), true);

    ASSERT_EQ("c2", field2->name());
    ASSERT_TRUE(field2->is_nullable());
}

TEST(FieldTest, test_with_type) {
    FieldPtr field1 = std::make_shared<Field>(1, std::string("c1"), get_type_info(OLAP_FIELD_TYPE_INT), false);
    FieldPtr field2 = field1->with_type(get_type_info(OLAP_FIELD_TYPE_BIGINT));

    ASSERT_EQ("c1", field2->name());
    ASSERT_FALSE(field2->is_nullable());
}

TEST(FieldTest, test_with_name) {
    FieldPtr field1 = std::make_shared<Field>(1, std::string("c1"), get_type_info(OLAP_FIELD_TYPE_INT), false);
    FieldPtr field2 = field1->with_name("c3");

    ASSERT_EQ("c3", field2->name());
    ASSERT_FALSE(field2->is_nullable());
}

TEST(FieldTest, test_with_nullable) {
    FieldPtr field1 = std::make_shared<Field>(1, std::string("c1"), get_type_info(OLAP_FIELD_TYPE_INT), false);
    FieldPtr field2 = field1->with_nullable(true);

    ASSERT_EQ("c1", field2->name());
    ASSERT_TRUE(field2->is_nullable());
}

TEST(FieldTest, test_copy) {
    FieldPtr field1 = std::make_shared<Field>(1, std::string("c1"), get_type_info(OLAP_FIELD_TYPE_INT), false);
    FieldPtr field2 = field1->copy();

    ASSERT_EQ("c1", field2->name());
    ASSERT_FALSE(field2->is_nullable());
}

} // namespace vectorized
} // namespace starrocks
