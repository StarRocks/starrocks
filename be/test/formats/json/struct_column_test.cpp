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

#include "formats/json/struct_column.h"

#include <gtest/gtest.h>

#include "column/struct_column.h"
#include "runtime/types.h"

namespace starrocks {

class AddStructColumnTest : public ::testing::Test {};

TEST_F(AddStructColumnTest, test_struct) {
    auto column = BinaryColumn::create();
    TypeDescriptor t = TypeDescriptor::create_struct_type(
            {"key2", "key3"}, {TypeDescriptor::create_varchar_type(10), TypeDescriptor::create_varchar_type(10)});

    simdjson::ondemand::parser parser;
    auto json = R"(  { "key1": "foo", "key2": "bar", "key3": "baz" }  )"_padded;
    auto doc = parser.iterate(json);
    simdjson::ondemand::value val = doc.find_field("key2");

    auto st = add_struct_column(column.get(), t, "key2", &val);
    ASSERT_TRUE(st.ok());

    ASSERT_EQ("['bar']", column->debug_string());
}

} // namespace starrocks
