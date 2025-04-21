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
#include <chrono>
#include <sstream>

#include "column/column_helper.h"
#include "gen_cpp/Descriptors_types.h"
#include "testutil/assert.h"

namespace starrocks::avrocpp {

class BinaryColumnReaderTest : public ::testing::Test {
public:
    ColumnReaderUniquePtr get_column_reader(const TypeDescriptor& type_desc, bool invalid_as_null) {
        return ColumnReader::get_nullable_column_reader(_col_name, type_desc, _timezone, invalid_as_null);
    }

    ColumnPtr create_adaptive_nullable_column(const TypeDescriptor& type_desc) {
        return ColumnHelper::create_column(type_desc, true, false, 0, true);
    }

private:
    std::string _col_name = "k1";
    cctz::time_zone _timezone = cctz::utc_time_zone();
};

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

} // namespace starrocks::avrocpp
