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

class NumericConverterTest : public ::testing::Test {
public:
    NumericConverterTest() { _type.type = TYPE_SMALLINT; }

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_read_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1", Converter::Options()));
    EXPECT_TRUE(
            conv->read_string(col.get(), std::to_string(std::numeric_limits<int16_t>::max()), Converter::Options()));
    EXPECT_TRUE(
            conv->read_string(col.get(), std::to_string(std::numeric_limits<int16_t>::lowest()), Converter::Options()));

    EXPECT_EQ(4, col->size());
    EXPECT_EQ(1, col->get(0).get_int16());
    EXPECT_EQ(-1, col->get(1).get_int16());
    EXPECT_EQ(std::numeric_limits<int16_t>::max(), col->get(2).get_int16());
    EXPECT_EQ(std::numeric_limits<int16_t>::lowest(), col->get(3).get_int16());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_read_float_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "1.1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1.999", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1.1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1.9999", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "32767.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-32768.0", Converter::Options()));

    EXPECT_EQ(6, col->size());
    EXPECT_EQ(1, col->get(0).get_int16());
    EXPECT_EQ(1, col->get(1).get_int16());
    EXPECT_EQ(-1, col->get(2).get_int16());
    EXPECT_EQ(-1, col->get(3).get_int16());
    EXPECT_EQ(32767, col->get(4).get_int16());
    EXPECT_EQ(-32768, col->get(5).get_int16());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_int_read_float_string) {
    auto conv = csv::get_converter(TypeDescriptor(TYPE_INT), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), false);

    EXPECT_TRUE(conv->read_string(col.get(), "2147483647.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "2147483647.000001", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "2147483647.99999", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-2147483648.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-2147483648.000001", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-2147483648.999999", Converter::Options()));

    EXPECT_EQ(6, col->size());
    EXPECT_EQ(2147483647, col->get(0).get_int32());
    EXPECT_EQ(2147483647, col->get(1).get_int32());
    EXPECT_EQ(2147483647, col->get(2).get_int32());
    EXPECT_EQ(-2147483647 - 1, col->get(3).get_int32());
    EXPECT_EQ(-2147483647 - 1, col->get(4).get_int32());
    EXPECT_EQ(-2147483647 - 1, col->get(5).get_int32());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_bigint_read_float_string) {
    auto conv = csv::get_converter(TypeDescriptor(TYPE_BIGINT), false);
    auto col = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), false);

    EXPECT_TRUE(conv->read_string(col.get(), "9223372036854775807.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "9223372036854775807.000001", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "9223372036854775807.99999", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-9223372036854775808.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-9223372036854775808.000001", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-9223372036854775808.999999", Converter::Options()));

    EXPECT_EQ(6, col->size());
    EXPECT_EQ(9223372036854775807, col->get(0).get_int64());
    EXPECT_EQ(9223372036854775807, col->get(1).get_int64());
    EXPECT_EQ(9223372036854775807, col->get(2).get_int64());
    EXPECT_EQ(-9223372036854775807 - 1, col->get(3).get_int64());
    EXPECT_EQ(-9223372036854775807 - 1, col->get(4).get_int64());
    EXPECT_EQ(-9223372036854775807 - 1, col->get(5).get_int64());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_read_string_overflow) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_FALSE(
            conv->read_string(col.get(), std::to_string(std::numeric_limits<int32_t>::max()), Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "32768", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "32768.0", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_read_string_underflow) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_FALSE(
            conv->read_string(col.get(), std::to_string(std::numeric_limits<int32_t>::lowest()), Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "-32769", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "-32769.0", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_read_quoted_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_quoted_string(col.get(), "1", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "-1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "32767.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-32768.0", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "32767.99999", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-32768.99999", Converter::Options()));

    EXPECT_EQ(6, col->size());
    EXPECT_EQ(1, col->get(0).get_int16());
    EXPECT_EQ(-1, col->get(1).get_int16());
    EXPECT_EQ(32767, col->get(2).get_int16());
    EXPECT_EQ(-32768, col->get(3).get_int16());
    EXPECT_EQ(32767, col->get(4).get_int16());
    EXPECT_EQ(-32768, col->get(5).get_int16());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_read_string_invalid_value) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_FALSE(conv->read_string(col.get(), "", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "abc", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "100a", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "a1000", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "\"100\"", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"100\"", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST_F(NumericConverterTest, test_write_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    col->append_datum((int16_t)-32768);
    col->append_datum((int16_t)11);
    col->append_datum((int16_t)32767);

    OutputStreamString buff(64);

    /// write_string

    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_EQ("", buff.as_string());

    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_EQ("", buff.as_string());

    ASSERT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_EQ("", buff.as_string());

    /// write_quoted_string

    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_EQ("", buff.as_string());

    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_EQ("", buff.as_string());

    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_EQ("", buff.as_string());

    buff.finalize();
    ASSERT_EQ("-327681132767-327681132767", buff.as_string());
}

} // namespace starrocks::csv
