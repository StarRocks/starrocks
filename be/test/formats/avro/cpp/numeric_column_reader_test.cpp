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

#include "formats/avro/cpp/numeric_column_reader.h"

#include <gtest/gtest.h>

#include <avrocpp/NodeImpl.hh>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "formats/avro/cpp/test_avro_utils.h"
#include "gen_cpp/Descriptors_types.h"

namespace starrocks::avrocpp {

class NumericColumnReaderTest : public ColumnReaderTest, public ::testing::Test {};

TEST_F(NumericColumnReaderTest, test_bool) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        bool bool_v = true;
        avro::GenericDatum datum(bool_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        bool bool_v = false;
        avro::GenericDatum datum(bool_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        int32_t int_v = 1;
        avro::GenericDatum datum(int_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        int64_t long_v = 0;
        avro::GenericDatum datum(long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "true";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "False";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(6, column->size());
    ASSERT_EQ("[1, 0, 1, 0, 1, 0]", column->debug_string());
}

TEST_F(NumericColumnReaderTest, test_numeric) {
    std::vector<TypeDescriptor> type_descs{
            TypeDescriptor::from_logical_type(TYPE_TINYINT), TypeDescriptor::from_logical_type(TYPE_SMALLINT),
            TypeDescriptor::from_logical_type(TYPE_INT), TypeDescriptor::from_logical_type(TYPE_BIGINT),
            TypeDescriptor::from_logical_type(TYPE_LARGEINT)};

    for (const auto& type_desc : type_descs) {
        auto column = create_adaptive_nullable_column(type_desc);
        auto reader = get_column_reader(type_desc, false);

        {
            bool bool_v = true;
            avro::GenericDatum datum(bool_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            int32_t int_v = 10;
            avro::GenericDatum datum(int_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            int64_t long_v = -11;
            avro::GenericDatum datum(long_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            std::string string_v = "12";
            avro::GenericDatum datum(string_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            float float_v = 13.1;
            avro::GenericDatum datum(float_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            double double_v = -14.2;
            avro::GenericDatum datum(double_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        ASSERT_EQ(6, column->size());
        ASSERT_EQ("[1, 10, -11, 12, 13, -14]", column->debug_string());
    }
}

TEST_F(NumericColumnReaderTest, test_float) {
    std::vector<TypeDescriptor> type_descs{TypeDescriptor::from_logical_type(TYPE_FLOAT),
                                           TypeDescriptor::from_logical_type(TYPE_DOUBLE)};

    for (const auto& type_desc : type_descs) {
        auto column = create_adaptive_nullable_column(type_desc);
        auto reader = get_column_reader(type_desc, false);

        {
            bool bool_v = true;
            avro::GenericDatum datum(bool_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            int32_t int_v = 10;
            avro::GenericDatum datum(int_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            int64_t long_v = -11;
            avro::GenericDatum datum(long_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            std::string string_v = "12";
            avro::GenericDatum datum(string_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            float float_v = 13.1;
            avro::GenericDatum datum(float_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        {
            double double_v = -14.2;
            avro::GenericDatum datum(double_v);
            CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
        }

        ASSERT_EQ(6, column->size());
        ASSERT_EQ("[1, 10, -11, 12, 13.1, -14.2]", column->debug_string());
    }
}

TEST_F(NumericColumnReaderTest, test_int_invalid) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_INT);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        int64_t long_v = 2147483648;
        avro::GenericDatum datum(long_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    ASSERT_EQ(0, column->size());
}

TEST_F(NumericColumnReaderTest, test_int_invalid_as_null) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_INT);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, true);

    {
        int64_t long_v = 2147483648;
        avro::GenericDatum datum(long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(2, column->size());
    ASSERT_EQ("[NULL, NULL]", column->debug_string());
}

TEST_F(NumericColumnReaderTest, test_decimal) {
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 10, 2);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        bool bool_v = true;
        avro::GenericDatum datum(bool_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        int32_t int_v = 10;
        avro::GenericDatum datum(int_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        int64_t long_v = 11;
        avro::GenericDatum datum(long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // negative
        int64_t long_v = -11;
        avro::GenericDatum datum(long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        double double_v = 12.11;
        avro::GenericDatum datum(double_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "13.2";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // bytes type, actual decimal value is 14.33
        int64_t decimal_v = 1433;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // bytes type, actual decimal value is -14.33
        int64_t decimal_v = -1433;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // bytes type, actual decimal value is 14.330, scale is different
        int64_t decimal_v = 14330;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(3);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // fixed type, actual decimal value is 15.34
        int64_t decimal_v = 1534;
        size_t fixed_size = 8;
        auto encoded_bytes = encode_decimal_bytes(decimal_v, fixed_size);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        auto node = avro::NodePtr(new avro::NodeFixed(avro::HasName(avro::Name(_col_name)), avro::HasSize(fixed_size)));
        node->setLogicalType(logical_type);

        avro::GenericFixed fixed(node, encoded_bytes);

        avro::GenericDatum datum(avro::AVRO_FIXED, logical_type, fixed);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // fixed type, actual decimal value is -15.34
        int64_t decimal_v = -1534;
        size_t fixed_size = 8;
        auto encoded_bytes = encode_decimal_bytes(decimal_v, fixed_size);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        auto node = avro::NodePtr(new avro::NodeFixed(avro::HasName(avro::Name(_col_name)), avro::HasSize(fixed_size)));
        node->setLogicalType(logical_type);

        avro::GenericFixed fixed(node, encoded_bytes);

        avro::GenericDatum datum(avro::AVRO_FIXED, logical_type, fixed);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(11, column->size());
    ASSERT_EQ("[1.00, 10.00, 11.00, -11.00, 12.11, 13.20, 14.33, -14.33, 14.33, 15.34, -15.34]",
              column->debug_string());
}

TEST_F(NumericColumnReaderTest, test_decimal_invalid) {
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 3, 1);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        // bytes type, actual decimal value is 14.31
        int64_t decimal_v = 1431;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    {
        // bytes type, actual decimal value is 114.3
        int64_t decimal_v = 1143;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(1);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    {
        // fixed type, actual decimal value is 14.31
        int64_t decimal_v = 1431;
        size_t fixed_size = 8;
        auto encoded_bytes = encode_decimal_bytes(decimal_v, fixed_size);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        auto node = avro::NodePtr(new avro::NodeFixed(avro::HasName(avro::Name(_col_name)), avro::HasSize(fixed_size)));
        node->setLogicalType(logical_type);

        avro::GenericFixed fixed(node, encoded_bytes);

        avro::GenericDatum datum(avro::AVRO_FIXED, logical_type, fixed);

        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    {
        // fixed type, actual decimal value is 114.3
        int64_t decimal_v = 1143;
        size_t fixed_size = 8;
        auto encoded_bytes = encode_decimal_bytes(decimal_v, fixed_size);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(1);

        auto node = avro::NodePtr(new avro::NodeFixed(avro::HasName(avro::Name(_col_name)), avro::HasSize(fixed_size)));
        node->setLogicalType(logical_type);

        avro::GenericFixed fixed(node, encoded_bytes);

        avro::GenericDatum datum(avro::AVRO_FIXED, logical_type, fixed);

        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    ASSERT_EQ(0, column->size());
}

TEST_F(NumericColumnReaderTest, test_decimal_invalid_as_null) {
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 3, 1);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, true);

    {
        // bytes type, actual decimal value is 14.31
        int64_t decimal_v = 1431;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // bytes type, actual decimal value is 114.3
        int64_t decimal_v = 1143;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(1);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // fixed type, actual decimal value is 14.31
        int64_t decimal_v = 1431;
        size_t fixed_size = 8;
        auto encoded_bytes = encode_decimal_bytes(decimal_v, fixed_size);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        auto node = avro::NodePtr(new avro::NodeFixed(avro::HasName(avro::Name(_col_name)), avro::HasSize(fixed_size)));
        node->setLogicalType(logical_type);

        avro::GenericFixed fixed(node, encoded_bytes);

        avro::GenericDatum datum(avro::AVRO_FIXED, logical_type, fixed);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // fixed type, actual decimal value is 114.3
        int64_t decimal_v = 1143;
        size_t fixed_size = 8;
        auto encoded_bytes = encode_decimal_bytes(decimal_v, fixed_size);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(1);

        auto node = avro::NodePtr(new avro::NodeFixed(avro::HasName(avro::Name(_col_name)), avro::HasSize(fixed_size)));
        node->setLogicalType(logical_type);

        avro::GenericFixed fixed(node, encoded_bytes);

        avro::GenericDatum datum(avro::AVRO_FIXED, logical_type, fixed);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(4, column->size());
    ASSERT_EQ("[NULL, NULL, NULL, NULL]", column->debug_string());
}

} // namespace starrocks::avrocpp
