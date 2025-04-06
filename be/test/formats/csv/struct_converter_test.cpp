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

#include "formats/csv/array_converter.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::csv {

// NOLINTNEXTLINE
TEST(StructConverterTest, test_hive_read_string01) {
    // STRUCT<TINYINT,VARCHAR>
    TypeDescriptor t(TYPE_STRUCT);
    t.children.emplace_back(TYPE_TINYINT);
    t.children.emplace_back(TYPE_VARCHAR);
    t.field_names.emplace_back("field1");
    t.field_names.emplace_back("field2");

    auto options = Converter::Options();
    options.hive_collection_delimiter = '_';
    options.hive_nested_level = 1;
    options.hive_mapkey_delimiter = '\003';

    auto conv = csv::get_hive_converter(t, true);
    auto col = ColumnHelper::create_column(t, true);

    EXPECT_TRUE(conv->read_string(col.get(), "10_", options));
    EXPECT_TRUE(conv->read_string(col.get(), "-1_apple", options));
    EXPECT_TRUE(conv->read_string(col.get(), "\\N_\\N", options));
    EXPECT_TRUE(conv->read_string(col.get(), "\\N_null", options));

    EXPECT_EQ(4, col->size());

    // {"field1":10,"field2":""}
    EXPECT_EQ("{field1:10,field2:''}", col->debug_item(0));

    // {"field1":-1,"field2":"apple"}
    EXPECT_EQ("{field1:-1,field2:'apple'}", col->debug_item(1));

    // {"field1":null,"field2":null}
    EXPECT_EQ("{field1:NULL,field2:NULL}", col->debug_item(2));

    // {"field1":null,"field2":"null"}
    EXPECT_EQ("{field1:NULL,field2:'null'}", col->debug_item(3));
}

// NOLINTNEXTLINE
TEST(StructConverterTest, test_hive_read_string02) {
    // STRUCT<STRUCT<TINYINT,VARCHAR>,VARCHAR>
    TypeDescriptor t(TYPE_STRUCT);
    t.children.emplace_back(TYPE_STRUCT);
    t.children.back().children.emplace_back(TYPE_TINYINT);
    t.children.back().children.emplace_back(TYPE_VARCHAR);
    t.field_names.emplace_back("field1");
    t.children.back().field_names.emplace_back("field11");
    t.children.back().field_names.emplace_back("field12");
    t.children.emplace_back(TYPE_VARCHAR);
    t.field_names.emplace_back("field2");

    auto options = Converter::Options();
    options.hive_collection_delimiter = '_';
    options.hive_nested_level = 1;
    options.hive_mapkey_delimiter = ':';

    auto conv = csv::get_hive_converter(t, true);
    auto col = ColumnHelper::create_column(t, true);

    EXPECT_TRUE(conv->read_string(col.get(), "-1:apple_", options));
    EXPECT_TRUE(conv->read_string(col.get(), "10:banana_test1", options));
    EXPECT_TRUE(conv->read_string(col.get(), "11:pear_\\N", options));

    EXPECT_EQ(3, col->size());

    // {{"field1":{"field11":-1,"field12":"apple"},"field2":""}}
    EXPECT_EQ("{field1:{field11:-1,field12:'apple'},field2:''}", col->debug_item(0));

    // {{"field1":{"field11":10,"field12":"banana"},"field2":"test1"}}
    EXPECT_EQ("{field1:{field11:10,field12:'banana'},field2:'test1'}", col->debug_item(1));

    // {{"field1":{"field11":10,"field12":"banana"},"field2":"test1"}}
    EXPECT_EQ("{field1:{field11:11,field12:'pear'},field2:NULL}", col->debug_item(2));
}

} // namespace starrocks::csv
