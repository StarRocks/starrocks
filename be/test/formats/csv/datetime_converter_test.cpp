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

class DatetimeConverterTest : public ::testing::Test {
public:
    DatetimeConverterTest() { _type.type = TYPE_DATETIME; }

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(DatetimeConverterTest, test_read_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "1990-01-01 01:02:03", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "19900101010203", Converter::Options()));

    EXPECT_EQ(2, col->size());
    EXPECT_EQ("1990-01-01 01:02:03", col->get(0).get_timestamp().to_string());
    EXPECT_EQ("1990-01-01 01:02:03", col->get(1).get_timestamp().to_string());
}

// NOLINTNEXTLINE
TEST_F(DatetimeConverterTest, test_read_string_invalid_value) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_FALSE(conv->read_string(col.get(), "", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "1", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "\"1990-11-25 01:02:03\"", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "DorisDB", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST_F(DatetimeConverterTest, test_read_quoted_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_quoted_string(col.get(), "\"1990-01-01 01:02:03\"", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "\"19900101010203\"", Converter::Options()));

    EXPECT_EQ(2, col->size());
    EXPECT_EQ("1990-01-01 01:02:03", col->get(0).get_timestamp().to_string());
    EXPECT_EQ("1990-01-01 01:02:03", col->get(1).get_timestamp().to_string());
}

// NOLINTNEXTLINE
TEST_F(DatetimeConverterTest, test_read_quoted_string_invalid_value) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_FALSE(conv->read_quoted_string(col.get(), "", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "1990-11-25 01:02:03", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"1990-11-25 01:02:03", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "1990-11-25 01:02:03\"", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"1\"", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"\"1990-11-25 01:02:03\"\"", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"DorisDB\"", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST_F(DatetimeConverterTest, test_write_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);
    csv::OutputStreamString buff;

    col->append_datum(TimestampValue::create(1999, 1, 1, 10, 9, 8));
    col->append_datum(TimestampValue::create(2000, 11, 20, 10, 9, 8));

    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("1999-01-01 10:09:082000-11-20 10:09:08\"1999-01-01 10:09:08\"\"2000-11-20 10:09:08\"", buff.as_string());
}

} // namespace starrocks::csv
