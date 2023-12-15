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

#include "formats/csv/map_converter.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::csv {

// NOLINTNEXTLINE
TEST(MapConverterTest, test_read_write_map_int_string) {
    // map<TINYINT,VARCHAR>
    TypeDescriptor t(TYPE_MAP);
    t.children.emplace_back(TYPE_TINYINT);
    t.children.emplace_back(TYPE_VARCHAR);
    t.children.back().len = 100;

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);
    // read
    EXPECT_TRUE(conv->read_string(col.get(), "{}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{1:\"abc\",2:NULL,3:\"\"}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{NULL:NULL}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{1:\"abc\",2:NULL,1:NULL}", Converter::Options())); // duplicated keys

    EXPECT_EQ(4, col->size());
    // {}
    EXPECT_EQ(0, col->get(0).get_map().size());
    EXPECT_EQ("{}", col->debug_item(0));
    // {1:abc,2:NULL,3:""}
    EXPECT_EQ(3, col->get(1).get_map().size());
    EXPECT_EQ("{1:'abc',2:NULL,3:''}", col->debug_item(1));
    // {NULL:NULL}
    EXPECT_EQ("{NULL:NULL}", col->debug_item(2));
    // {1:NULL,2:NULL}
    EXPECT_EQ(2, col->get(3).get_map().size());
    EXPECT_EQ("{2:NULL,1:NULL}", col->debug_item(3));

    // write
    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 3, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("{}{1:\"abc\",2:null,3:\"\"}{null:null}{2:null,1:null}", buff.as_string());
}

// NOLINTNEXTLINE
TEST(MapConverterTest, test_read_write_nest_map) {
    // map<int,map<TINYINT,VARCHAR>>
    TypeDescriptor n(TYPE_MAP);
    n.children.emplace_back(TYPE_TINYINT);
    n.children.emplace_back(TYPE_VARCHAR);
    n.children.back().len = 100;
    TypeDescriptor t(TYPE_MAP);
    t.children.emplace_back(TYPE_INT);
    t.children.push_back(n);

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(conv->read_string(col.get(), "{}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{1:{}}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{1:{1:\"abc\",2:NULL},2:{3:\"\"}}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{NULL:{NULL:NULL}}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{NULL:{NULL:\"NULL\"},-20308764:{33:\"\"}}", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "{11:{1:\"abc\",2:NULL,1:NULL},22:{NULL:NULL},22:{2:\"unique\"}}",
                                  Converter::Options())); // duplicated keys

    EXPECT_EQ(6, col->size());
    // {}
    EXPECT_EQ(0, col->get(0).get_map().size());
    // {1:{}}
    EXPECT_EQ(1, col->get(1).get_map().size());
    EXPECT_EQ("{1:{}}", col->debug_item(1));
    // {1:{1:"abc",2:NULL},2:{3:""}}
    EXPECT_EQ(2, col->get(2).get_map().size());
    EXPECT_EQ("{1:{1:'abc',2:NULL},2:{3:''}}", col->debug_item(2));
    // {NULL:{NULL:NULL}}
    EXPECT_EQ("{NULL:{NULL:NULL}}", col->debug_item(3));
    // {NULL:{NULL:"NULL"},-20308764:{33:""}}
    EXPECT_EQ("{NULL:{NULL:'NULL'},-20308764:{33:''}}", col->debug_item(4));
    // {11:{1:"abc",2:NULL,1:NULL},22:{NULL:NULL},22:{2:"unique"}}
    EXPECT_EQ(2, col->get(5).get_map().size());
    EXPECT_EQ("{11:{2:NULL,1:NULL},22:{2:'unique'}}", col->debug_item(5));

    // write
    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 3, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 4, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 5, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ(
            "{}{1:{}}{1:{1:\"abc\",2:null},2:{3:\"\"}}{null:{null:null}}{null:{null:\"NULL\"},-20308764:{33:\"\"}}{11:{"
            "2:null,1:null},22:{2:\"unique\"}}",
            buff.as_string());
}

} // namespace starrocks::csv
