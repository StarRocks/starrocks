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

// NOLINTNEXTLINE
TEST(BooleanConverterTest, test_read_string) {
    auto conv = csv::get_converter(TypeDescriptor(TYPE_BOOLEAN), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);

    EXPECT_TRUE(conv->read_string(col.get(), "1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "10", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "true", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "TRUE", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "tRue", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " tRue", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "false", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "False", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), " FALSE", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1.1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1.9", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1.1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1.9", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "0.0001", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-0.0001", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "0.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "9999999999999999999999999.999", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "0.000000000000001", Converter::Options()));

    EXPECT_EQ(20, col->size());
    EXPECT_EQ(1, col->get(0).get_int8());
    EXPECT_EQ(1, col->get(1).get_int8());
    EXPECT_EQ(1, col->get(2).get_int8());
    EXPECT_EQ(0, col->get(3).get_int8());
    EXPECT_EQ(1, col->get(4).get_int8());
    EXPECT_EQ(1, col->get(5).get_int8());
    EXPECT_EQ(1, col->get(6).get_int8());
    EXPECT_EQ(1, col->get(7).get_int8());
    EXPECT_EQ(0, col->get(8).get_int8());
    EXPECT_EQ(0, col->get(9).get_int8());
    EXPECT_EQ(0, col->get(10).get_int8());
    EXPECT_EQ(1, col->get(11).get_int8());
    EXPECT_EQ(1, col->get(12).get_int8());
    EXPECT_EQ(1, col->get(13).get_int8());
    EXPECT_EQ(1, col->get(14).get_int8());
    EXPECT_EQ(1, col->get(15).get_int8());
    EXPECT_EQ(1, col->get(16).get_int8());
    EXPECT_EQ(0, col->get(17).get_int8());
    EXPECT_EQ(1, col->get(18).get_int8());
    EXPECT_EQ(1, col->get(19).get_int8());
}

// NOLINTNEXTLINE
TEST(BooleanConverterTest, test_read_string_invalid_value) {
    auto conv = csv::get_converter(TypeDescriptor(TYPE_BOOLEAN), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);
    EXPECT_FALSE(conv->read_string(col.get(), "1a", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST(BooleanConverterTest, test_read_quoted_string) {
    auto conv = csv::get_converter(TypeDescriptor(TYPE_BOOLEAN), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);

    EXPECT_TRUE(conv->read_quoted_string(col.get(), "1", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "10", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "-1", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "0", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "true", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), " TRUE", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "false", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), " false", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "False", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "100.123", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "-678.912", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "-0.00001", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "0.00001", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "0.0", Converter::Options()));

    EXPECT_EQ(14, col->size());
    EXPECT_EQ(1, col->get(0).get_int8());
    EXPECT_EQ(1, col->get(1).get_int8());
    EXPECT_EQ(1, col->get(2).get_int8());
    EXPECT_EQ(0, col->get(3).get_int8());
    EXPECT_EQ(1, col->get(4).get_int8());
    EXPECT_EQ(1, col->get(5).get_int8());
    EXPECT_EQ(0, col->get(6).get_int8());
    EXPECT_EQ(0, col->get(7).get_int8());
    EXPECT_EQ(0, col->get(8).get_int8());
    EXPECT_EQ(1, col->get(9).get_int8());
    EXPECT_EQ(1, col->get(10).get_int8());
    EXPECT_EQ(1, col->get(11).get_int8());
    EXPECT_EQ(1, col->get(12).get_int8());
    EXPECT_EQ(0, col->get(13).get_int8());
}

// NOLINTNEXTLINE
TEST(BooleanConverterTest, test_read_quoted_string_invalid_value) {
    auto conv = csv::get_converter(TypeDescriptor(TYPE_BOOLEAN), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);

    EXPECT_FALSE(conv->read_quoted_string(col.get(), "1a", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"1\"", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"0\"", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST(BooleanConverterTest, test_write_string_as_integral_value) {
    csv::Converter::Options opts{.bool_alpha = false};
    auto conv = csv::get_converter(TypeDescriptor(TYPE_BOOLEAN), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);

    col->append_datum(true);
    col->append_datum(false);
    col->append_datum(true);
    col->append_datum(false);

    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, opts).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, opts).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, opts).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 3, opts).ok());
    // not flushed yet.
    ASSERT_EQ("", buff.as_string());

    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 0, opts).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 1, opts).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 2, opts).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 3, opts).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("10101010", buff.as_string());
}

// NOLINTNEXTLINE
TEST(BooleanConverterTest, test_write_string_as_textual_value) {
    csv::Converter::Options opts{.bool_alpha = true};
    auto conv = csv::get_converter(TypeDescriptor(TYPE_BOOLEAN), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BOOLEAN), false);

    col->append_datum(true);
    col->append_datum(false);
    col->append_datum(true);
    col->append_datum(false);

    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, opts).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, opts).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, opts).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 3, opts).ok());
    // not flushed yet.
    ASSERT_EQ("", buff.as_string());

    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 0, opts).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 1, opts).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 2, opts).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 3, opts).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("truefalsetruefalsetruefalsetruefalse", buff.as_string());
}

} // namespace starrocks::csv
