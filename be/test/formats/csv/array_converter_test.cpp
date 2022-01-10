// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::vectorized::csv {

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_read_string) {
    // ARRAY<TINYINT>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_TINYINT);

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(conv->read_string(col.get(), "[]", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "[10]", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "[-1]", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "[1,2,3,null]", Converter::Options()));

    EXPECT_EQ(4, col->size());
    // []
    EXPECT_EQ(0, col->get(0).get_array().size());
    // [10]
    EXPECT_EQ(1, col->get(1).get_array().size());
    EXPECT_EQ(10, col->get(1).get_array()[0].get_int8());
    // [-1]
    EXPECT_EQ(1, col->get(2).get_array().size());
    EXPECT_EQ(-1, col->get(2).get_array()[0].get_int8());
    // [1,2,3,null]
    EXPECT_EQ(4, col->get(3).get_array().size());
    EXPECT_EQ(1, col->get(3).get_array()[0].get_int8());
    EXPECT_EQ(2, col->get(3).get_array()[1].get_int8());
    EXPECT_EQ(3, col->get(3).get_array()[2].get_int8());
    EXPECT_TRUE(col->get(3).get_array()[3].is_null());
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_read_string01) {
    // ARRAY<STRING>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_VARCHAR);
    t.children.back().len = 6000;

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(conv->read_string(col.get(), "[]", Converter::Options()));
    EXPECT_TRUE(conv->read_string(col.get(), "[\"apple\",\"banana\",null]", Converter::Options()));

    EXPECT_EQ(2, col->size());
    // []
    EXPECT_EQ(0, col->get(0).get_array().size());
    // ["apple","banana",null]
    EXPECT_EQ(3, col->get(1).get_array().size());
    EXPECT_EQ("apple", col->get(1).get_array()[0].get_slice());
    EXPECT_EQ("banana", col->get(1).get_array()[1].get_slice());
    EXPECT_TRUE(col->get(1).get_array()[2].is_null());
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_read_string02) {
    // ARRAY<STRING>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_VARCHAR);
    t.children.back().len = 6000;

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(conv->read_string(col.get(), "[\"a,b,c\",\"\",\"I\"\"m\"]", Converter::Options()));

    EXPECT_EQ(1, col->size());
    // ["a,b,c","","I""m"]
    EXPECT_EQ(3, col->get(0).get_array().size());
    EXPECT_EQ("a,b,c", col->get(0).get_array()[0].get_slice());
    EXPECT_EQ("", col->get(0).get_array()[1].get_slice());
    EXPECT_EQ("I\"m", col->get(0).get_array()[2].get_slice());
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_read_string03) {
    // ARRAY<STRING>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_VARCHAR);
    t.children.back().len = 6000;

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_FALSE(conv->read_string(col.get(), "[apple,banana]", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "[\"banana\"", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "\"banana\"]", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "[\"banana]", Converter::Options()));
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_read_string04) {
    // ARRAY<ARRAY<STRING>>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_ARRAY);
    t.children.back().children.emplace_back(TYPE_VARCHAR);
    t.children.back().children.back().len = 6000;

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(
            conv->read_string(col.get(), "[[],[null],[\"apple\",null],[\"banana\",\"pear\"]]", Converter::Options()));
    EXPECT_EQ(1, col->size());

    auto arr = col->get(0).get_array();
    EXPECT_EQ(4, arr.size());

    // []
    EXPECT_EQ(0, arr[0].get_array().size());

    // [null]
    EXPECT_EQ(1, arr[1].get_array().size());
    EXPECT_TRUE(arr[1].get_array()[0].is_null());

    // ["apple",null]
    EXPECT_EQ(2, arr[2].get_array().size());
    EXPECT_EQ("apple", arr[2].get_array()[0].get_slice());
    EXPECT_TRUE(arr[2].get_array()[1].is_null());

    // ["banana","pear"]
    EXPECT_EQ(2, arr[3].get_array().size());
    EXPECT_EQ("banana", arr[3].get_array()[0].get_slice());
    EXPECT_EQ("pear", arr[3].get_array()[1].get_slice());
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_read_string05) {
    // ARRAY<DATE>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_DATE);

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(conv->read_string(col.get(), "[\"1990-11-25\",\"2021-02-22\"]", Converter::Options()));
    EXPECT_FALSE(conv->read_string(col.get(), "[1990-11-25,2021-02-22]", Converter::Options()));

    EXPECT_EQ(1, col->size());
    EXPECT_EQ(2, col->get(0).get_array().size());
    EXPECT_EQ("1990-11-25", col->get(0).get_array()[0].get_date().to_string());
    EXPECT_EQ("2021-02-22", col->get(0).get_array()[1].get_date().to_string());
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_write_string) {
    // ARRAY<DATE>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_DATE);

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    csv::OutputStreamString buff;
    col->append_datum(DatumArray{});
    col->append_datum(DatumArray{Datum()}); // [NULL]
    col->append_datum(DatumArray{DateValue::create(1999, 9, 1)});
    col->append_datum(DatumArray{DateValue::create(2000, 9, 1), DateValue::create(2001, 9, 1)});

    ASSERT_TRUE(conv->write_string(&buff, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_string(&buff, *col, 3, Converter::Options()).ok());
    ASSERT_TRUE(buff.finalize().ok());
    ASSERT_EQ("[][null][\"1999-09-01\"][\"2000-09-01\",\"2001-09-01\"]", buff.as_string());

    csv::OutputStreamString buff2;
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 0, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 1, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 2, Converter::Options()).ok());
    ASSERT_TRUE(conv->write_quoted_string(&buff2, *col, 3, Converter::Options()).ok());
    ASSERT_TRUE(buff2.finalize().ok());
    ASSERT_EQ("[][null][\"1999-09-01\"][\"2000-09-01\",\"2001-09-01\"]", buff2.as_string());
}

} // namespace starrocks::vectorized::csv