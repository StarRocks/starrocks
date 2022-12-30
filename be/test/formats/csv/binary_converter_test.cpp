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

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::csv {

class BinaryConverterTest : public ::testing::Test {
public:
    BinaryConverterTest() {
        _type.type = TYPE_VARCHAR;
        _type.len = 6000;
    }

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "Foo", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "Bar", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "\"Database\"", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "String with escape \\", Converter::Options()));

    EXPECT_EQ(4, col->size());
    EXPECT_EQ("Foo", col->get(0).get_slice());
    EXPECT_EQ("Bar", col->get(1).get_slice());
    EXPECT_EQ("\"Database\"", col->get(2).get_slice());
    EXPECT_EQ("String with escape \\", col->get(3).get_slice());
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_large_string01) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    std::string large_string(TypeDescriptor::MAX_VARCHAR_LENGTH + 1, 'x');
    EXPECT_FALSE(conv->read_string(col.get(), large_string, Converter::Options()));
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_large_string02) {
    TypeDescriptor varchar_type;
    varchar_type.type = TYPE_VARCHAR;
    varchar_type.len = 10;

    auto conv = csv::get_converter(varchar_type, false);
    auto col = ColumnHelper::create_column(varchar_type, false);

    std::string large_string("helloworldx");
    Converter::Options options;
    options.type_desc = &varchar_type;
    EXPECT_FALSE(conv->read_string(col.get(), large_string, options));
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_quoted_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_quoted_string(col.get(), "\"Foo\"", Converter::Options()));
    EXPECT_TRUE(
            conv->read_quoted_string(col.get(), "\"String with two successive quotes \"\".\"", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "\"String with escaped quote \\\".\"", Converter::Options()));

    EXPECT_EQ(3, col->size());
    EXPECT_EQ("Foo", col->get(0).get_slice());
    EXPECT_EQ("String with two successive quotes \".", col->get(1).get_slice());
    // FIXME(zhuming): unexpected behaviour
    // EXPECT_EQ("String with escaped quote \"", col->get(2).get_slice());
    EXPECT_EQ("String with escaped quote \\\".", col->get(2).get_slice());
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_large_quoted_string01) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    std::string large_string(TypeDescriptor::MAX_VARCHAR_LENGTH, 'x');
    std::string quoted_string;
    quoted_string.reserve(large_string.size() + 2);
    quoted_string.push_back('"');
    quoted_string.append(large_string);
    quoted_string.push_back('"');
    EXPECT_TRUE(conv->read_quoted_string(col.get(), quoted_string, Converter::Options()));
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_large_quoted_string02) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    std::string large_string(TypeDescriptor::MAX_VARCHAR_LENGTH + 1, 'x');
    std::string quoted_string;
    quoted_string.reserve(large_string.size() + 2);
    quoted_string.push_back('"');
    quoted_string.append(large_string);
    quoted_string.push_back('"');
    EXPECT_FALSE(conv->read_quoted_string(col.get(), quoted_string, Converter::Options()));
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_large_quoted_string03) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    std::string large_string(TypeDescriptor::MAX_VARCHAR_LENGTH + 1, 'x');
    // Two double quotas represent a single double quota, so the actual length of large_string
    // is MAX_VARCHAR_LENGTH.
    large_string[100] = '"';
    large_string[101] = '"';
    std::string quoted_string;
    quoted_string.reserve(large_string.size() + 2);
    quoted_string.push_back('"');
    quoted_string.append(large_string);
    quoted_string.push_back('"');
    EXPECT_TRUE(conv->read_quoted_string(col.get(), quoted_string, Converter::Options()));
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_read_large_quoted_string04) {
    TypeDescriptor varchar_type;
    varchar_type.type = TYPE_VARCHAR;
    varchar_type.len = 10;

    auto conv = csv::get_converter(varchar_type, false);
    auto col = ColumnHelper::create_column(varchar_type, false);

    std::string large_string("helloworldx");
    std::string quoted_string;
    quoted_string.reserve(large_string.size() + 2);
    quoted_string.push_back('"');
    quoted_string.append(large_string);
    quoted_string.push_back('"');

    Converter::Options options;
    options.type_desc = &varchar_type;
    EXPECT_FALSE(conv->read_quoted_string(col.get(), quoted_string, options));
}

// NOLINTNEXTLINE
TEST_F(BinaryConverterTest, test_write_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);
    (void)col->append_strings({"aaaaaaaaaaaa", "bbbbbbbb", "\"\"", "ccccc"});

    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 3, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("aaaaaaaaaaaabbbbbbbb\"\"ccccc", buff.as_string());

    csv::OutputStreamString buff2;
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 3, Converter::Options()).ok());

    ASSERT_TRUE(buff2.finalize().ok());
    ASSERT_EQ("\"aaaaaaaaaaaa\"\"bbbbbbbb\"\"\"\"\"\"\"\"ccccc\"", buff2.as_string());
}

} // namespace starrocks::csv
