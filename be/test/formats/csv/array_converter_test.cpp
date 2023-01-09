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

#include "formats/csv/array_converter.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "formats/csv/converter.h"
#include "formats/csv/output_stream_string.h"
#include "runtime/types.h"

namespace starrocks::csv {

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_read_string01) {
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
TEST(ArrayConverterTest, test_read_string02) {
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
TEST(ArrayConverterTest, test_read_string03) {
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
TEST(ArrayConverterTest, test_read_string04) {
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
TEST(ArrayConverterTest, test_read_string05) {
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
TEST(ArrayConverterTest, test_read_string06) {
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
TEST(ArrayConverterTest, test_hive_read_string01) {
    // ARRAY<TINYINT>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_TINYINT);

    auto options = Converter::Options();
    options.array_format_type = ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = '_';
    options.array_hive_nested_level = 1;
    options.array_hive_mapkey_delimiter = '\003';

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(conv->read_string(col.get(), "", options));
    EXPECT_TRUE(conv->read_string(col.get(), "10", options));
    EXPECT_TRUE(conv->read_string(col.get(), "-1", options));
    EXPECT_TRUE(conv->read_string(col.get(), "1_2_3_\\N", options));

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
TEST(ArrayConverterTest, test_hive_read_string02) {
    // ARRAY<STRING>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_VARCHAR);
    t.children.back().len = 6000;

    auto options = Converter::Options();
    options.array_format_type = ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = '_';
    options.array_hive_nested_level = 1;
    options.array_hive_mapkey_delimiter = '\003';

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    EXPECT_TRUE(conv->read_string(col.get(), "", options));
    EXPECT_TRUE(conv->read_string(col.get(), "apple_banana_\\N", options));

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
TEST(ArrayConverterTest, test_hive_read_string03) {
    // ARRAY<ARRAY<STRING>>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_ARRAY);
    t.children.back().children.emplace_back(TYPE_VARCHAR);
    t.children.back().children.back().len = 6000;

    auto options = Converter::Options();
    options.array_format_type = ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = '_';
    options.array_hive_nested_level = 1;
    options.array_hive_mapkey_delimiter = ':';

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    // [[],[null],["apple",null],["banana","pear"]]
    EXPECT_TRUE(conv->read_string(col.get(), "_\\N_apple:\\N_banana:pear", options));
    EXPECT_EQ(1, col->size());

    auto arr = col->get(0).get_array();
    EXPECT_EQ(4, arr.size());

    // []
    EXPECT_EQ(0, arr[0].get_array().size());

    // [null] NOTE!!!: In Hive, [null] will be parsed to null directly.
    EXPECT_TRUE(arr[1].is_null());

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
TEST(ArrayConverterTest, test_hive_read_string04) {
    // ARRAY<ARRAY<ARRAY<STRING>>>
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_ARRAY);
    t.children.back().children.emplace_back(TYPE_ARRAY);
    t.children.back().children.back().children.emplace_back(TYPE_VARCHAR);
    t.children.back().children.back().children.back().len = 6000;

    auto options = Converter::Options();
    options.array_format_type = ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = '\002';
    options.array_hive_nested_level = 1;
    options.array_hive_mapkey_delimiter = '\003';

    auto conv = csv::get_converter(t, false);
    auto col = ColumnHelper::create_column(t, false);

    // [[[null, "smith"]]]
    EXPECT_TRUE(conv->read_string(col.get(), "\\N\004smith", options));
    EXPECT_EQ(1, col->size());

    auto arr = col->get(0).get_array()[0].get_array()[0].get_array();
    EXPECT_EQ(2, arr.size());
    EXPECT_TRUE(arr[0].is_null());
    EXPECT_EQ("smith", arr[1].get_slice());
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_hive_read_string05) {
    // Super complex Hive array test

    auto options = Converter::Options();
    options.array_format_type = ArrayFormatType::kHive;
    options.array_hive_collection_delimiter = '\002';
    options.array_hive_nested_level = 1;
    options.array_hive_mapkey_delimiter = ':';

    {
        // ARRAY<ARRAY<TINYINT>>
        TypeDescriptor t(TYPE_ARRAY);
        t.children.emplace_back(TYPE_ARRAY);
        t.children.back().children.emplace_back(TYPE_TINYINT);

        auto conv = csv::get_converter(t, false);
        auto col = ColumnHelper::create_column(t, false);

        // [[1, 2, 3], [4, 5, 6], null, [7, 8, null]]
        EXPECT_TRUE(conv->read_string(col.get(), "1:2:3\0024:5:6\002\\N\0027:8:\\N", options));
        EXPECT_EQ(1, col->size());

        auto arr = col->get(0).get_array();

        auto arr0 = arr[0].get_array();
        EXPECT_EQ(1, arr0[0].get_int8());
        EXPECT_EQ(2, arr0[1].get_int8());
        EXPECT_EQ(3, arr0[2].get_int8());

        auto arr1 = arr[1].get_array();
        EXPECT_EQ(4, arr1[0].get_int8());
        EXPECT_EQ(5, arr1[1].get_int8());
        EXPECT_EQ(6, arr1[2].get_int8());

        auto arr2 = arr[2];
        EXPECT_TRUE(arr2.is_null());

        auto arr3 = arr[3].get_array();
        EXPECT_EQ(7, arr3[0].get_int8());
        EXPECT_EQ(8, arr3[1].get_int8());
        EXPECT_TRUE(arr3[2].is_null());
    }
    {
        // ARRAY<ARRAY<ARRAY<VARCHAR>>>
        TypeDescriptor t(TYPE_ARRAY);
        t.children.emplace_back(TYPE_ARRAY);
        t.children.back().children.emplace_back(TYPE_ARRAY);
        t.children.back().children.back().children.emplace_back(TYPE_VARCHAR);
        t.children.back().children.back().children.back().len = 6000;

        auto conv = csv::get_converter(t, false);
        auto col = ColumnHelper::create_column(t, false);

        // [[["null", "a", null], ["a"]], null, null]
        EXPECT_TRUE(conv->read_string(col.get(), "null\004a\004\\N:a\002\\N\002\\N", options));
        EXPECT_EQ(1, col->size());

        auto arr = col->get(0).get_array();

        // ["null", "a", null]
        auto arr0 = arr[0].get_array();
        auto arr00 = arr0[0].get_array();
        EXPECT_EQ("null", arr00[0].get_slice());
        EXPECT_EQ("a", arr00[1].get_slice());
        EXPECT_TRUE(arr00[2].is_null());

        // ["a"]
        auto arr01 = arr0[1].get_array();
        EXPECT_EQ("a", arr01[0].get_slice());

        // [null]
        auto arr1 = arr[1];
        EXPECT_TRUE(arr1.is_null());

        // [null]
        auto arr2 = arr[2];
        EXPECT_TRUE(arr2.is_null());
    }
    {
        // ARRAY<VARCHAR>
        TypeDescriptor t(TYPE_ARRAY);
        t.children.emplace_back(TYPE_VARCHAR);
        t.children.back().len = 5000;

        auto conv = csv::get_converter(t, false);
        auto col = ColumnHelper::create_column(t, false);

        // ["null", "a", null]
        EXPECT_TRUE(conv->read_string(col.get(), "null\002a\002\\N", options));
        EXPECT_EQ(1, col->size());

        auto arr = col->get(0).get_array();
        EXPECT_EQ("null", arr[0].get_slice());
        EXPECT_EQ("a", arr[1].get_slice());
        EXPECT_TRUE(arr[2].is_null());
    }
    {
        // ARRAY<VARCHAR>
        TypeDescriptor t(TYPE_ARRAY);
        t.children.emplace_back(TYPE_VARCHAR);
        t.children.back().len = 5000;

        auto conv = csv::get_converter(t, false);
        auto col = ColumnHelper::create_column(t, false);

        // []
        EXPECT_TRUE(conv->read_string(col.get(), "", options));
        EXPECT_EQ(1, col->size());

        auto arr = col->get(0).get_array();
        EXPECT_EQ(0, arr.size());
    }
}

// NOLINTNEXTLINE
TEST(ArrayConverterTest, test_get_collection_delimiter) {
    // Rule refer to:
    // https://github.com/apache/hive/blob/90428cc5f594bd0abb457e4e5c391007b2ad1cb8/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
    std::vector<char> separators;

    for (char i = 1; i <= 8; ++i) {
        separators.push_back(i);
    }

    separators.push_back(11);

    for (char i = 14; i <= 26; i++) {
        separators.push_back(i);
    }

    for (char i = 28; i <= 31; i++) {
        separators.push_back(i);
    }

    for (char i = -128; i <= -1; i++) {
        separators.push_back(i);
    }

    const char DEFAULT_COLLECTION_DELIMITER = '\002';
    const char DEFAULT_MAPKEY_DELIMITER = '\003';

    // Start to check, ignore first element in separators, because array delimiter start from second element.
    for (size_t i = 1, nested_array_level = 1; i < separators.size(); i++) {
        char delimiter = HiveTextArrayReader::get_collection_delimiter(DEFAULT_COLLECTION_DELIMITER,
                                                                       DEFAULT_MAPKEY_DELIMITER, nested_array_level++);
        EXPECT_EQ(delimiter, separators[i]);
    }
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

} // namespace starrocks::csv
