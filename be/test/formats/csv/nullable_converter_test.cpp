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

class NullableConverterTest : public ::testing::Test {
public:
    NullableConverterTest() { _type.type = TYPE_INT; }

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(NullableConverterTest, test_read_string) {
    auto conv = csv::get_converter(_type, true);
    auto col = ColumnHelper::create_column(_type, true);

    EXPECT_TRUE(conv->read_string(col.get(), "1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-1", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "\\N", Converter::Options()));

    EXPECT_EQ(3, col->size());
    EXPECT_EQ(1, col->get(0).get_int32());
    EXPECT_EQ(-1, col->get(1).get_int32());
    EXPECT_TRUE(col->get(2).is_null());
}

// NOLINTNEXTLINE
TEST_F(NullableConverterTest, test_read_quoted_string) {
    auto conv = csv::get_converter(_type, true);
    auto col = ColumnHelper::create_column(_type, true);

    EXPECT_TRUE(conv->read_quoted_string(col.get(), "1", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "-1", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "null", Converter::Options()));

    EXPECT_EQ(3, col->size());
    EXPECT_EQ(1, col->get(0).get_int32());
    EXPECT_EQ(-1, col->get(1).get_int32());
    EXPECT_TRUE(col->get(2).is_null());
}

// NOLINTNEXTLINE
TEST_F(NullableConverterTest, test_read_string_invalid_value_as_null) {
    auto conv = csv::get_converter(_type, true);
    auto col = ColumnHelper::create_column(_type, true);

    EXPECT_TRUE(conv->read_string(col.get(), "", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "abc", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "100a", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "a1000", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "\"100\"", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "\"100\"", Converter::Options()));

    EXPECT_EQ(6, col->size());
    EXPECT_TRUE(col->get(0).is_null());
    EXPECT_TRUE(col->get(1).is_null());
    EXPECT_TRUE(col->get(2).is_null());
    EXPECT_TRUE(col->get(3).is_null());
    EXPECT_TRUE(col->get(4).is_null());
    EXPECT_TRUE(col->get(5).is_null());
}

// NOLINTNEXTLINE
TEST_F(NullableConverterTest, test_read_string_invalid_value_as_error) {
    auto conv = csv::get_converter(_type, true);
    auto col = ColumnHelper::create_column(_type, true);

    Converter::Options opts{.invalid_field_as_null = false};

    EXPECT_FALSE(conv->read_string(col.get(), "", opts));
    EXPECT_FALSE(conv->read_string(col.get(), "abc", opts));
    EXPECT_FALSE(conv->read_string(col.get(), "100a", opts));
    EXPECT_FALSE(conv->read_string(col.get(), "a1000", opts));
    EXPECT_FALSE(conv->read_string(col.get(), "\"100\"", opts));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"100\"", opts));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST_F(NullableConverterTest, test_write_string) {
    auto conv = csv::get_converter(_type, true);
    auto col = ColumnHelper::create_column(_type, true);
    col->append_datum(Datum()); // null
    col->append_datum((int32_t)10);

    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("\\N10null10", buff.as_string());
}

} // namespace starrocks::csv
