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
#include <random>

#include "base/testutil/assert.h"
#include "base/time/timezone_utils.h"
#include "column/adaptive_nullable_column.h"
#include "column/column_helper.h"
#include "formats/avro/cpp/direct_column_reader.h"
#include "formats/avro/cpp/test_avro_utils.h"
#include "gen_cpp/Descriptors_types.h"

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

// timestamp-millis / timestamp-micros values read as DATETIME (generic datums and the direct decoder) and as
// VARCHAR show the instant in the reader zone with the offset at that instant, the same as the cctz conversion,
// across DST changes, before 1970 and in any order.
TEST_F(DateColumnReaderTest, test_datetime_dst_timezone_matches_cctz) {
    std::mt19937_64 rng(23);
    std::vector<int64_t> seconds = {0, -1, 1711846800 - 1, 1711846800, 1729990800 - 1, 1729990800};
    for (int i = 0; i < 2000; i++) {
        seconds.push_back(static_cast<int64_t>(rng() % 4000000000ULL) - 1000000000LL);
    }
    for (const std::string zone : {"America/Los_Angeles", "Europe/London", "Asia/Shanghai"}) {
        cctz::time_zone timezone;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(zone, timezone));
        for (bool millis : {true, false}) {
            SCOPED_TRACE(zone + (millis ? " millis" : " micros"));
            const int64_t per_second = millis ? 1000 : 1000000;
            const auto logical = millis ? avro::LogicalType::TIMESTAMP_MILLIS : avro::LogicalType::TIMESTAMP_MICROS;
            std::vector<int64_t> values;
            std::vector<std::string> want;
            for (int64_t second : seconds) {
                const int64_t sub = static_cast<int64_t>(rng() % per_second);
                values.push_back(second * per_second + (second < 0 ? -sub : sub));
                TimestampValue tv;
                tv.from_unixtime(values.back() / per_second, values.back() % per_second * (1000000 / per_second),
                                 timezone);
                want.push_back(tv.to_string());
            }

            // generic datums, as DATETIME and as VARCHAR
            for (LogicalType type : {TYPE_DATETIME, TYPE_VARCHAR}) {
                auto type_desc = type == TYPE_VARCHAR ? TypeDescriptor::create_varchar_type(64)
                                                      : TypeDescriptor::from_logical_type(TYPE_DATETIME);
                auto column = create_adaptive_nullable_column(type_desc);
                auto reader = get_column_reader(type_desc, timezone, false);
                for (int64_t v : values) {
                    avro::GenericDatum datum(avro::AVRO_LONG, avro::LogicalType(logical), v);
                    ASSERT_OK(reader->read_datum_for_adaptive_column(datum, column.get()));
                }
                ASSERT_EQ(values.size(), column->size());
                for (size_t i = 0; i < values.size(); i++) {
                    std::string got = column->debug_item(i);
                    if (type == TYPE_VARCHAR) {
                        ASSERT_EQ("'" + want[i] + "'", got) << values[i];
                    } else {
                        ASSERT_EQ(want[i], got) << values[i];
                    }
                }
            }

            // the direct decoder
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
            auto type_desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);
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
