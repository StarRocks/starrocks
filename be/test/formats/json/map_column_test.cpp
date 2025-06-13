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

#include "formats/json/map_column.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "runtime/types.h"
#include "testutil/assert.h"

namespace starrocks {

class AddMapColumnTest : public ::testing::Test {};

TEST_F(AddMapColumnTest, test_good_json) {
    TypeDescriptor type_desc = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(10),
                                                               TypeDescriptor::create_varchar_type(10));

    auto column = ColumnHelper::create_column(type_desc, false);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "key1": "foo", "key2": "bar", "key3": "baz" }  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.get_value();

    EXPECT_OK(add_map_column(column.get(), type_desc, "root_key", &val));

    EXPECT_EQ("{'key1':'foo','key2':'bar','key3':'baz'}", column->debug_string());
}

TEST_F(AddMapColumnTest, test_bad_json) {
    TypeDescriptor type_desc = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(10),
                                                               TypeDescriptor::create_varchar_type(10));

    auto column = ColumnHelper::create_column(type_desc, false);

    simdjson::ondemand::parser parser;
    auto json = R"(  { "key1": "foo", "key2": "bar" "key3": "baz"  } )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.get_value();

    EXPECT_STATUS(Status::DataQualityError(""), add_map_column(column.get(), type_desc, "root_key", &val));
}

} // namespace starrocks
