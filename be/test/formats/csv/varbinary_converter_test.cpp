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

#include <gtest/gtest.h>

#include <iomanip>
#include <set>
#include <sstream>

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::csv {

class VarBinaryConverterTest : public ::testing::Test {
public:
    VarBinaryConverterTest() {
        _type.type = TYPE_VARBINARY;
        _type.len = 6000;
    }

    std::string hex_binary(const Slice& str) {
        std::stringstream ss;
        ss << std::hex << std::uppercase << std::setfill('0');
        for (int i = 0; i < str.size; ++i) {
            ss << std::setw(2) << (static_cast<int32_t>(str.data[i]) & 0xFF);
        }
        return ss.str();
    }

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_read_varbinary) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "AB", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "0101", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " AB", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " 0101", Converter::Options()));

    EXPECT_EQ(4, col->size());
    ASSERT_EQ("AB", hex_binary(col->get(0).get_slice()));
    ASSERT_EQ("0101", hex_binary(col->get(1).get_slice()));
    ASSERT_EQ("AB", hex_binary(col->get(2).get_slice()));
    ASSERT_EQ("0101", hex_binary(col->get(3).get_slice()));

    EXPECT_FALSE(conv->read_string(col.get(), "xyz", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "1", Converter::Options()));
}

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_read_large_binary01) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    std::string large_string(TypeDescriptor::MAX_VARCHAR_LENGTH * 2 + 1, 'a');
    EXPECT_FALSE(conv->read_string(col.get(), large_string, Converter::Options()));
}

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_read_large_binary02) {
    TypeDescriptor binary_type;
    binary_type.type = TYPE_VARBINARY;
    binary_type.len = 10;

    auto conv = csv::get_converter(binary_type, false);
    auto col = ColumnHelper::create_column(binary_type, false);

    std::string large_string("xxxxx");
    Converter::Options options;
    options.type_desc = &binary_type;
    EXPECT_FALSE(conv->read_string(col.get(), large_string, options));
}

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_write_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);
    (void)col->append_strings({"aaaaaaaaaaaa", "bbbbbbbb", "", "ccccc"});

    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 3, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("61616161616161616161616162626262626262626363636363", buff.as_string());

    csv::OutputStreamString buff2;
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 3, Converter::Options()).ok());

    ASSERT_TRUE(buff2.finalize().ok());
    ASSERT_EQ("\"616161616161616161616161\"\"6262626262626262\"\"\"\"6363636363\"", buff2.as_string());
}

} // namespace starrocks::csv
