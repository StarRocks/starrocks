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

#include "formats/avro/cpp/date_column_reader.h"

#include <gtest/gtest.h>

#include <avrocpp/Compiler.hh>
#include <avrocpp/Decoder.hh>
#include <avrocpp/Encoder.hh>
#include <avrocpp/NodeImpl.hh>
#include <avrocpp/Stream.hh>

#include "column/adaptive_nullable_column.h"
#include "column/column_helper.h"
#include "formats/avro/cpp/direct_column_reader.h"
#include "formats/avro/cpp/test_avro_utils.h"
#include "gen_cpp/Descriptors_types.h"
#include "testutil/assert.h"
#include "util/timezone_utils.h"

namespace starrocks::avrocpp {

class DateColumnReaderTest : public ColumnReaderTest, public ::testing::Test {
protected:
    using ColumnReaderTest::get_column_reader;

private:
    ColumnReaderUniquePtr get_column_reader(const TypeDescriptor& type_desc, const cctz::time_zone& timezone,
                                            bool invalid_as_null) {
        return ColumnReader::get_nullable_column_reader(_col_name, type_desc, timezone, invalid_as_null);
    }
};

TEST_F(DateColumnReaderTest, test_date) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATE);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        int32_t int_v = days_since_epoch("2025-04-16");
        avro::GenericDatum datum(avro::AVRO_INT, avro::LogicalType(avro::LogicalType::DATE), int_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "2025-04-17";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(2, column->size());
    ASSERT_EQ("[2025-04-16, 2025-04-17]", column->debug_string());
}

TEST_F(DateColumnReaderTest, test_date_invalid) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATE);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    ASSERT_EQ(0, column->size());
}

TEST_F(DateColumnReaderTest, test_date_invalid_as_null) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATE);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, true);

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[NULL]", column->debug_string());
}

TEST_F(DateColumnReaderTest, test_datetime) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        int64_t long_v = milliseconds_since_epoch("2025-04-16 12:01:01.123");
        avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MILLIS), long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        int64_t long_v = microseconds_since_epoch("2025-04-16 12:01:01.123456");
        avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS), long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        std::string string_v = "2025-04-16 12:01:01.123789";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(3, column->size());
    ASSERT_EQ("[2025-04-16 12:01:01.123000, 2025-04-16 12:01:01.123456, 2025-04-16 12:01:01.123789]",
              column->debug_string());
}

TEST_F(DateColumnReaderTest, test_datetime_timezone) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    auto column = create_adaptive_nullable_column(type_desc);
    cctz::time_zone timezone;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", timezone));
    auto reader = get_column_reader(type_desc, timezone, false);

    {
        int64_t long_v = milliseconds_since_epoch("2025-04-16 12:01:01.123");
        avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MILLIS), long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    {
        int64_t long_v = microseconds_since_epoch("2025-04-16 12:01:01.123456");
        avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS), long_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(2, column->size());
    ASSERT_EQ("[2025-04-16 20:01:01.123000, 2025-04-16 20:01:01.123456]", column->debug_string());
}

// timestamp-millis / timestamp-micros values before 1970 with a fraction of a second keep their instant, through
// both generic datums and the direct decoder: the seconds are floored, so the microsecond part is never negative.
TEST_F(DateColumnReaderTest, test_datetime_before_epoch) {
    const cctz::time_zone timezone = cctz::utc_time_zone();
    for (bool millis : {true, false}) {
        SCOPED_TRACE(millis ? "millis" : "micros");
        const auto logical = millis ? avro::LogicalType::TIMESTAMP_MILLIS : avro::LogicalType::TIMESTAMP_MICROS;
        const std::vector<int64_t> values =
                millis ? std::vector<int64_t>{-1, -1500, -1000, 1} : std::vector<int64_t>{-1, -1500000, -1000000, 1};
        const std::vector<std::string> want =
                millis ? std::vector<std::string>{"1969-12-31 23:59:59.999000", "1969-12-31 23:59:58.500000",
                                                  "1969-12-31 23:59:59", "1970-01-01 00:00:00.001000"}
                       : std::vector<std::string>{"1969-12-31 23:59:59.999999", "1969-12-31 23:59:58.500000",
                                                  "1969-12-31 23:59:59", "1970-01-01 00:00:00.000001"};
        auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);

        // generic datums
        {
            auto column = create_adaptive_nullable_column(type_desc);
            auto reader = get_column_reader(type_desc, timezone, false);
            for (int64_t v : values) {
                avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(logical), v);
                ASSERT_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
            }
            ASSERT_EQ(values.size(), column->size());
            for (size_t i = 0; i < values.size(); i++) {
                ASSERT_EQ(want[i], column->debug_item(i)) << values[i];
            }
        }

        // the direct decoder
        {
            avro::ValidSchema schema =
                    avro::compileJsonSchemaFromString(std::string(R"({"type": "long", "logicalType": ")") +
                                                      (millis ? "timestamp-millis" : "timestamp-micros") + R"("})");
            auto out = avro::memoryOutputStream();
            auto encoder = avro::binaryEncoder();
            encoder->init(*out);
            for (int64_t v : values) {
                encoder->encodeLong(v);
            }
            encoder->flush();
            auto in = avro::memoryInputStream(*out);
            auto decoder = avro::binaryDecoder();
            decoder->init(*in);
            auto direct = DirectColumnReader::make(_col_name, type_desc, schema.root(), timezone);
            ASSERT_TRUE(direct != nullptr);
            auto column = create_adaptive_nullable_column(type_desc);
            auto* adaptive = down_cast<AdaptiveNullableColumn*>(column.get());
            for (size_t i = 0; i < values.size(); i++) {
                ASSERT_OK(direct->read_field(*decoder, adaptive));
            }
            ASSERT_EQ(values.size(), column->size());
            for (size_t i = 0; i < values.size(); i++) {
                ASSERT_EQ(want[i], column->debug_item(i)) << values[i];
            }
        }
    }
}

TEST_F(DateColumnReaderTest, test_datetime_invalid) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, false);

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        auto st = reader->read_datum_for_adaptive_column(datum, column.get());
        ASSERT_TRUE(st.is_data_quality_error());
    }

    ASSERT_EQ(0, column->size());
}

TEST_F(DateColumnReaderTest, test_datetime_invalid_as_null) {
    auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);
    auto column = create_adaptive_nullable_column(type_desc);
    auto reader = get_column_reader(type_desc, true);

    {
        std::string string_v = "abc";
        avro::GenericDatum datum(string_v);
        CHECK_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
    }

    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[NULL]", column->debug_string());
}

} // namespace starrocks::avrocpp
