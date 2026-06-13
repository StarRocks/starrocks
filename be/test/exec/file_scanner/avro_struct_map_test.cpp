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

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <avrocpp/Compiler.hh>
#include <vector>

#include "column/column_helper.h"
#include "exec/file_scanner/confluent_avro_decoder.h"
#include "formats/avro/cpp/column_reader.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

// Proves STRUCT and MAP end-to-end on the routine-load decode path: decode a message into a
// GenericDatum (ConfluentAvroDecoder), then fill columns with the same avrocpp column readers the
// scanner uses, and check the rendered values. No pipe/scanner needed.
class AvroStructMapTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const char* kJson = R"({
            "type": "record",
            "name": "r",
            "fields": [
                {"name": "s", "type": {"type": "record", "name": "inner",
                                       "fields": [{"name": "a", "type": "long"},
                                                  {"name": "b", "type": "string"}]}},
                {"name": "m", "type": {"type": "map", "values": "long"}}
            ]
        })";
        _decoder = std::make_unique<ConfluentAvroDecoder>(avro::compileJsonSchemaFromString(kJson));
        ASSERT_TRUE(_decoder->init().ok());

        // {s: {a: 1, b: "x"}, m: {"k1": 10, "k2": 20}}
        //   s.a   long 1     -> 0x02
        //   s.b   string "x" -> len 1 (0x02) + 'x' (0x78)
        //   m     map block count 2 (0x04),
        //           "k1" (len 2 -> 0x04) 'k'(0x6B)'1'(0x31), value 10 (0x14),
        //           "k2" (len 2 -> 0x04) 'k'(0x6B)'2'(0x32), value 20 (0x28),
        //         end block 0 (0x00)
        const std::vector<uint8_t> bytes = {0x02, 0x02, 0x78, 0x04, 0x04, 0x6B, 0x31,
                                            0x14, 0x04, 0x6B, 0x32, 0x28, 0x00};
        ASSERT_TRUE(_decoder->decode(bytes.data(), bytes.size(), &_datum).ok());
        _tz = cctz::utc_time_zone();
    }

    std::unique_ptr<ConfluentAvroDecoder> _decoder;
    avro::GenericDatum _datum;
    cctz::time_zone _tz;
};

TEST_F(AvroStructMapTest, struct_field) {
    auto type = TypeDescriptor::create_struct_type(
            {"a", "b"}, {TypeDescriptor(TYPE_BIGINT), TypeDescriptor::create_varchar_type(16)});
    auto reader = avrocpp::ColumnReader::get_nullable_column_reader("s", type, _tz, /*invalid_as_null=*/true);
    auto column = ColumnHelper::create_column(type, true);

    const auto& field = _datum.value<avro::GenericRecord>().field("s");
    ASSERT_TRUE(reader->read_datum(field, column.get()).ok());
    EXPECT_EQ("{a:1,b:'x'}", column->debug_item(0));
}

TEST_F(AvroStructMapTest, map_field) {
    auto type = TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(16), TypeDescriptor(TYPE_BIGINT));
    auto reader = avrocpp::ColumnReader::get_nullable_column_reader("m", type, _tz, /*invalid_as_null=*/true);
    auto column = ColumnHelper::create_column(type, true);

    const auto& field = _datum.value<avro::GenericRecord>().field("m");
    ASSERT_TRUE(reader->read_datum(field, column.get()).ok());
    EXPECT_EQ("{'k1':10,'k2':20}", column->debug_item(0));
}

TEST_F(AvroStructMapTest, map_with_non_string_key_column_is_rejected) {
    // Avro map keys are always strings; a MAP<INT,...> column must yield a data error (NULL in
    // non-strict mode), not raw string bytes appended into the integer key column.
    auto type = TypeDescriptor::create_map_type(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_BIGINT));
    const auto& field = _datum.value<avro::GenericRecord>().field("m");

    auto strict_reader = avrocpp::ColumnReader::get_nullable_column_reader("m", type, _tz, /*invalid_as_null=*/false);
    auto column = ColumnHelper::create_column(type, true);
    auto st = strict_reader->read_datum(field, column.get());
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is_data_quality_error());

    auto lax_reader = avrocpp::ColumnReader::get_nullable_column_reader("m", type, _tz, /*invalid_as_null=*/true);
    auto lax_column = ColumnHelper::create_column(type, true);
    ASSERT_TRUE(lax_reader->read_datum(field, lax_column.get()).ok());
    EXPECT_EQ("NULL", lax_column->debug_item(0));
}

TEST_F(AvroStructMapTest, kind_mismatched_datum_is_rejected) {
    // The reader is built from the table column type while the datum carries whatever the writer
    // sent: a scalar datum routed into a STRUCT/MAP/ARRAY column must yield a data error (NULL in
    // non-strict mode), not a crashing value<T>() cast.
    const auto& scalar = _datum.value<avro::GenericRecord>().field("s").value<avro::GenericRecord>().field("a");
    const std::vector<TypeDescriptor> types = {
            TypeDescriptor::create_struct_type({"a"}, {TypeDescriptor(TYPE_BIGINT)}),
            TypeDescriptor::create_map_type(TypeDescriptor::create_varchar_type(16), TypeDescriptor(TYPE_BIGINT)),
            TypeDescriptor::create_array_type(TypeDescriptor(TYPE_BIGINT)),
    };
    for (const auto& type : types) {
        auto strict_reader =
                avrocpp::ColumnReader::get_nullable_column_reader("c", type, _tz, /*invalid_as_null=*/false);
        auto column = ColumnHelper::create_column(type, true);
        auto st = strict_reader->read_datum(scalar, column.get());
        ASSERT_FALSE(st.ok()) << type.debug_string();
        EXPECT_TRUE(st.is_data_quality_error()) << type.debug_string();

        auto lax_reader = avrocpp::ColumnReader::get_nullable_column_reader("c", type, _tz, /*invalid_as_null=*/true);
        auto lax_column = ColumnHelper::create_column(type, true);
        ASSERT_TRUE(lax_reader->read_datum(scalar, lax_column.get()).ok()) << type.debug_string();
        EXPECT_EQ("NULL", lax_column->debug_item(0)) << type.debug_string();
    }
}

// The decoder must preserve the Avro logicalType so DateColumnReader interprets a logical `date`
// (int days since epoch) as a DATE, rather than the raw day count the legacy reader would yield.
TEST_F(AvroStructMapTest, logical_date) {
    auto schema = avro::compileJsonSchemaFromString(R"({
        "type": "record",
        "name": "r",
        "fields": [{"name": "d", "type": {"type": "int", "logicalType": "date"}}]
    })");
    ConfluentAvroDecoder decoder(schema);
    ASSERT_TRUE(decoder.init().ok());

    // date = 5 days since epoch -> 1970-01-06; int 5 -> zigzag 10 -> 0x0A
    const std::vector<uint8_t> bytes = {0x0A};
    avro::GenericDatum datum;
    ASSERT_TRUE(decoder.decode(bytes.data(), bytes.size(), &datum).ok());

    auto type = TypeDescriptor(TYPE_DATE);
    auto reader = avrocpp::ColumnReader::get_nullable_column_reader("d", type, _tz, /*invalid_as_null=*/true);
    auto column = ColumnHelper::create_column(type, true);
    ASSERT_TRUE(reader->read_datum(datum.value<avro::GenericRecord>().field("d"), column.get()).ok());
    EXPECT_EQ("1970-01-06", column->debug_item(0));
}

} // namespace starrocks
