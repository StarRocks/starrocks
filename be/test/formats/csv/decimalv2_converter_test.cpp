// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::vectorized::csv {

class DecimalV2ConverterTest : public ::testing::Test {
public:
    DecimalV2ConverterTest() {
        _type.type = TYPE_DECIMALV2;
        _type.precision = 20;
        _type.scale = 10;
    }

protected:
    TypeDescriptor _type;
};

// NOLINTNEXTLINE
TEST_F(DecimalV2ConverterTest, test_read_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_string(col.get(), "999999999999.000000001", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "-999999999999.000000001", Converter::Options()));

    EXPECT_EQ(2, col->size());
    EXPECT_EQ("999999999999.000000001", col->get(0).get_decimal().to_string());
    EXPECT_EQ("-999999999999.000000001", col->get(1).get_decimal().to_string());
}

// NOLINTNEXTLINE
TEST_F(DecimalV2ConverterTest, test_read_quoted_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_TRUE(conv->read_quoted_string(col.get(), "999999999999.000000001", Converter::Options()));
    EXPECT_TRUE(conv->read_quoted_string(col.get(), "-999999999999.000000001", Converter::Options()));

    EXPECT_EQ(2, col->size());
    EXPECT_EQ("999999999999.000000001", col->get(0).get_decimal().to_string());
    EXPECT_EQ("-999999999999.000000001", col->get(1).get_decimal().to_string());
}

// NOLINTNEXTLINE
TEST_F(DecimalV2ConverterTest, test_read_string_invalid_value) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);

    EXPECT_FALSE(conv->read_string(col.get(), "", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "abc", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "a1000", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "\"100\"", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "100a", Converter::Options()));
    EXPECT_FALSE(conv->read_quoted_string(col.get(), "\"100\"", Converter::Options()));

    EXPECT_EQ(0, col->size());
}

// NOLINTNEXTLINE
TEST_F(DecimalV2ConverterTest, test_write_string) {
    auto conv = csv::get_converter(_type, false);
    auto col = ColumnHelper::create_column(_type, false);
    col->append_datum(DecimalV2Value("0.00000001"));
    col->append_datum(DecimalV2Value("-0.00000001"));
    col->append_datum(DecimalV2Value("99999999999999999.00000001"));

    csv::OutputStreamString buff;
    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("0.00000001-0.0000000199999999999999999.00000001", buff.as_string());

    csv::OutputStreamString buff2;
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(buff2.finalize().ok());
    ASSERT_EQ("0.00000001-0.0000000199999999999999999.00000001", buff2.as_string());
}

} // namespace starrocks::vectorized::csv
