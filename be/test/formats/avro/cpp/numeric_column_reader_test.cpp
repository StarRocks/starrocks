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

#include "column/column_helper.h"
#include "gen_cpp/Descriptors_types.h"
#include "testutil/assert.h"

namespace starrocks {

class NumericColumnReaderTest : public ::testing::Test {
public:
    avrocpp::ColumnReaderUniquePtr get_column_reader(const TypeDescriptor& type_desc, bool invalid_as_null) {
        return avrocpp::ColumnReader::get_nullable_column_reader(_col_name, type_desc, _timezone, invalid_as_null);
    }

    MutableColumnPtr create_adaptive_nullable_column(const TypeDescriptor& type_desc) {
        return ColumnHelper::create_column(type_desc, true, false, 0, true);
    }

    std::vector<uint8_t> encode_decimal_bytes(int64_t unscaled_value, size_t fixed_size = 0) {
        std::vector<uint8_t> bytes;
        bool is_negative = unscaled_value < 0;
        uint64_t abs_value = std::abs(unscaled_value);

        while (abs_value > 0) {
            bytes.insert(bytes.begin(), abs_value & 0xFF);
            abs_value >>= 8;
        }

        if (bytes.empty()) {
            bytes.push_back(0);
        }
        if (fixed_size > bytes.size()) {
            size_t padding = fixed_size - bytes.size();
            uint8_t pad_byte = is_negative ? 0xFF : 0x00;
            bytes.insert(bytes.begin(), padding, pad_byte);
        }

        return bytes;
    }

private:
    std::string _col_name = "k1";
    cctz::time_zone _timezone = cctz::utc_time_zone();
};

TEST_F(NumericColumnReaderTest, test_int) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_INT);
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
        std::string string_v = "12";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(4, column->size());
    ASSERT_EQ("[1, 10, 11, 12]", column->debug_string());
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
        // actual decimal value is 14.33
        int64_t decimal_v = 1433;
        auto encoded_bytes = encode_decimal_bytes(decimal_v);
        avro::LogicalType logical_type(avro::LogicalType::DECIMAL);
        logical_type.setPrecision(10);
        logical_type.setScale(2);
        avro::GenericDatum datum(avro::AVRO_BYTES, logical_type, encoded_bytes);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(6, column->size());
    ASSERT_EQ("[1.00, 10.00, 11.00, 12.11, 13.20, 14.33]", column->debug_string());
}

} // namespace starrocks
