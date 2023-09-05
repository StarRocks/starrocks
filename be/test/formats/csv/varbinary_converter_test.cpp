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

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_read_varbinary) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "YWJjZA==", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "YQ== ", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " ZWZnaGlq", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " ZWZnaGlqX2s= ", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "   ", Converter::Options()));

    EXPECT_EQ(5, col->size());
    EXPECT_EQ(Slice("abcd"), col->get(0).get_slice());
    EXPECT_EQ(Slice("a"), col->get(1).get_slice());
    EXPECT_EQ(Slice("efghij"), col->get(2).get_slice());
    EXPECT_EQ(Slice("efghij_k"), col->get(3).get_slice());
    EXPECT_EQ(Slice(), col->get(4).get_slice());
}

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_read_varbinary_fallback) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "YWJjZA==.", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "YQ== !", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " ZWZnaGlq.", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " ZWZnaGlqX2s= ?", Converter::Options()));

    EXPECT_EQ(4, col->size());
    EXPECT_EQ(Slice("YWJjZA==."), col->get(0).get_slice());
    EXPECT_EQ(Slice("YQ== !"), col->get(1).get_slice());
    EXPECT_EQ(Slice(" ZWZnaGlq."), col->get(2).get_slice());
    EXPECT_EQ(Slice(" ZWZnaGlqX2s= ?"), col->get(3).get_slice());
}

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_read_large_binary01) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    std::string large_string(TypeDescriptor::MAX_VARCHAR_LENGTH * 4, 'a');
    EXPECT_FALSE(conv->read_string(col.get(), large_string, Converter::Options()));
}

// NOLINTNEXTLINE
TEST_F(VarBinaryConverterTest, test_read_large_binary02) {
    TypeDescriptor binary_type;
    binary_type.type = TYPE_VARBINARY;
    binary_type.len = 5;

    auto conv = csv::get_converter(binary_type, false);
    auto col = ColumnHelper::create_column(binary_type, false);

    std::string large_string("ZWZnaGlqX2s=");
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
    EXPECT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    EXPECT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    EXPECT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    EXPECT_TRUE(conv->write_string(&buff, *col, 3, Converter::Options()).ok());
    EXPECT_TRUE(buff.finalize().ok());
    EXPECT_EQ("YWFhYWFhYWFhYWFhYmJiYmJiYmI=Y2NjY2M=", buff.as_string());

    csv::OutputStreamString buff2;
    EXPECT_TRUE(conv->write_quoted_string(&buff2, *col, 0, Converter::Options()).ok());
    EXPECT_TRUE(conv->write_quoted_string(&buff2, *col, 1, Converter::Options()).ok());
    EXPECT_TRUE(conv->write_quoted_string(&buff2, *col, 2, Converter::Options()).ok());
    EXPECT_TRUE(conv->write_quoted_string(&buff2, *col, 3, Converter::Options()).ok());

    EXPECT_TRUE(buff2.finalize().ok());
    EXPECT_EQ("\"YWFhYWFhYWFhYWFh\"\"YmJiYmJiYmI=\"\"\"\"Y2NjY2M=\"", buff2.as_string());
}

} // namespace starrocks::csv
