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
#include "types/logical_type.h"

namespace starrocks::csv {

class FloatConverterTest : public ::testing::Test {
public:
    FloatConverterTest() { _type.type = TYPE_DOUBLE; }

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(FloatConverterTest, test_read_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "0.01", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-0.01", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1e-2", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1E-2", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1e-2", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1E-2", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1.23e-3", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1.23E-3", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1E2", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "1e2", Converter::Options()));

    EXPECT_EQ(10, col->size());
    EXPECT_FLOAT_EQ(0.01, col->get(0).get_double());
    EXPECT_FLOAT_EQ(-0.01, col->get(1).get_double());
    EXPECT_FLOAT_EQ(0.01, col->get(2).get_double());
    EXPECT_FLOAT_EQ(0.01, col->get(3).get_double());
    EXPECT_FLOAT_EQ(-0.01, col->get(4).get_double());
    EXPECT_FLOAT_EQ(-0.01, col->get(5).get_double());
    EXPECT_FLOAT_EQ(0.00123, col->get(6).get_double());
    EXPECT_FLOAT_EQ(0.00123, col->get(7).get_double());
    EXPECT_FLOAT_EQ(100, col->get(8).get_double());
    EXPECT_FLOAT_EQ(100, col->get(9).get_double());
}

// NOLINTNEXTLINE
TEST_F(FloatConverterTest, test_read_quoted_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_quoted_string(col.get(), "0.01", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "-0.01", Converter::Options()));

    EXPECT_EQ(2, col->size());
    EXPECT_FLOAT_EQ(0.01, col->get(0).get_double());
    EXPECT_FLOAT_EQ(-0.01, col->get(1).get_double());
}

// NOLINTNEXTLINE
TEST_F(FloatConverterTest, test_read_string_invalid_value) {
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
TEST_F(FloatConverterTest, test_double_write_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);
    col->append_datum((double)1.1);
    col->append_datum((double)-0.1);

    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("1.1-0.11.1-0.1", buff.as_string());

    csv::OutputStreamString buff2;
    auto col2 = ColumnHelper::create_column(_type, false);
    col2->append_datum((double)1.12345678901234e+18);
    ASSERT_TRUE(conv->write_string(&buff2, *col2, 0, Converter::Options()).ok());
    ASSERT_TRUE(buff2.finalize().ok());
    ASSERT_EQ("1.12345678901234e+18", buff2.as_string());
}

TEST_F(FloatConverterTest, test_float_write_string) {
    TypeDescriptor type(TYPE_FLOAT);
    auto conv = csv::get_converter(type, false);
    auto col = ColumnHelper::create_column(type, false);
    col->append_datum((float)7.092579e+08);
    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("7.092579e+08", buff.as_string());
}

} // namespace starrocks::csv
