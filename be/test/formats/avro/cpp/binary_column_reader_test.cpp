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

#include "formats/avro/cpp/binary_column_reader.h"

#include <gtest/gtest.h>

#include <avrocpp/NodeImpl.hh>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "formats/avro/cpp/test_avro_utils.h"
#include "gen_cpp/Descriptors_types.h"

namespace starrocks::avrocpp {

class BinaryColumnReaderTest : public ColumnReaderTest, public ::testing::Test {};

TEST_F(BinaryColumnReaderTest, test_normal) {
    auto type_desc = TypeDescriptor::create_varchar_type(1024);
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
        float float_v = 11.1;
        avro::GenericDatum datum(float_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        double double_v = 11.2;
        avro::GenericDatum datum(double_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "def";
        std::vector<uint8_t> bytes_v(string_v.begin(), string_v.end());
        avro::GenericDatum datum(bytes_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // enum
        avro::LeafNames symbols;
        symbols.add("a");
        symbols.add("b");
        auto node = avro::NodePtr(new avro::NodeEnum(avro::HasName(avro::Name(_col_name)), symbols));

        avro::GenericEnum enum_v(node, "b");

        avro::GenericDatum datum(avro::AVRO_ENUM, avro::LogicalType(avro::LogicalType::NONE), enum_v);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(8, column->size());
    ASSERT_EQ("['1', '10', '11', '11.100000', '11.200000', 'abc', 'def', 'b']", column->debug_string());
}

TEST_F(BinaryColumnReaderTest, test_invalid) {
    auto type_desc = TypeDescriptor::create_varchar_type(2);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        int32_t int_v = 100;
        avro::GenericDatum datum(int_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    {
        float float_v = 11.1;
        avro::GenericDatum datum(float_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    {
        std::string string_v = "def";
        std::vector<uint8_t> bytes_v(string_v.begin(), string_v.end());
        avro::GenericDatum datum(bytes_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    ASSERT_EQ(0, column->size());
}

TEST_F(BinaryColumnReaderTest, test_invalid_as_null) {
    auto type_desc = TypeDescriptor::create_varchar_type(2);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, true);

    {
        int32_t int_v = 100;
        avro::GenericDatum datum(int_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        float float_v = 11.1;
        avro::GenericDatum datum(float_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "def";
        std::vector<uint8_t> bytes_v(string_v.begin(), string_v.end());
        avro::GenericDatum datum(bytes_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(4, column->size());
    ASSERT_EQ("[NULL, NULL, NULL, NULL]", column->debug_string());
}

TEST_F(BinaryColumnReaderTest, test_from_complex_type) {
    auto type_desc = TypeDescriptor::create_varchar_type(1024);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        // record
        avro::MultiLeaves field_nodes;
        field_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT)));
        field_nodes.add(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_STRING)));

        avro::LeafNames field_names;
        field_names.add("int_col");
        field_names.add("string_col");

        std::vector<avro::GenericDatum> datums;
        int32_t int_v = 10;
        datums.emplace_back(avro::GenericDatum(int_v));
        std::string string_v = "abc";
        datums.emplace_back(avro::GenericDatum(string_v));

        auto record_schema = avro::NodePtr(
                new avro::NodeRecord(avro::HasName(avro::Name(_col_name)), field_nodes, field_names, datums));

        auto record_datum = avro::GenericRecord(record_schema);
        for (size_t i = 0; i < datums.size(); ++i) {
            record_datum.setFieldAt(i, datums[i]);
        }

        auto datum = avro::GenericDatum(record_schema, record_datum);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // array
        avro::SingleLeaf item_node(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT)));
        auto array_schema = avro::NodePtr(new avro::NodeArray(item_node));

        auto array_datum = avro::GenericArray(array_schema);
        auto& array_value = array_datum.value();
        {
            int32_t int_v = 10;
            array_value.emplace_back(avro::GenericDatum(int_v));
        }
        {
            int32_t int_v = 11;
            array_value.emplace_back(avro::GenericDatum(int_v));
        }

        auto datum = avro::GenericDatum(array_schema, array_datum);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // map
        avro::SingleLeaf value_node(avro::NodePtr(new avro::NodePrimitive(avro::AVRO_INT)));
        auto map_schema = avro::NodePtr(new avro::NodeMap(value_node));

        auto map_datum = avro::GenericMap(map_schema);
        auto& map_value = map_datum.value();
        {
            int32_t int_v = 10;
            map_value.emplace_back("abc", avro::GenericDatum(int_v));
        }
        {
            int32_t int_v = 11;
            map_value.emplace_back("def", avro::GenericDatum(int_v));
        }

        auto datum = avro::GenericDatum(map_schema, map_datum);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(3, column->size());
    ASSERT_EQ("['{\"int_col\":10,\"string_col\":\"abc\"}', '[10,11]', '{\"abc\":10,\"def\":11}']",
              column->debug_string());
}

TEST_F(BinaryColumnReaderTest, test_from_logical_type) {
    auto type_desc = TypeDescriptor::create_varchar_type(1024);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        // date
        int32_t int_v = days_since_epoch("2025-04-16");
        avro::GenericDatum datum(avro::AVRO_INT, avro::LogicalType(avro::LogicalType::DATE), int_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // timestamp millis
        int64_t long_v = milliseconds_since_epoch("2025-04-16 12:01:01.123");
        avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MILLIS), long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // timestamp micros
        int64_t long_v = microseconds_since_epoch("2025-04-16 12:01:01.123456");
        avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS), long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // bytes decimal
        int64_t decimal_v = 1433;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);

        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);

        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);

        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        // fixed decimal
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

    ASSERT_EQ(5, column->size());
    ASSERT_EQ("['2025-04-16', '2025-04-16 12:01:01.123000', '2025-04-16 12:01:01.123456', '14.330000', '15.340000']",
              column->debug_string());
}

} // namespace starrocks::avrocpp
