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

#include "formats/parquet/calendar_rebase.h"

#include <gtest/gtest.h>

#include <cstring>
#include <limits>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/statistics_helper.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "util/int96.h"

namespace starrocks::parquet {

namespace {
tparquet::KeyValue kv(const std::string& key, const std::string& value) {
    tparquet::KeyValue result;
    result.__set_key(key);
    result.__set_value(value);
    return result;
}

ParquetField leaf(tparquet::Type::type physical_type) {
    ParquetField field;
    field.name = "c";
    field.physical_type = physical_type;
    field.schema_element.__set_name("c");
    field.schema_element.__set_type(physical_type);
    return field;
}

ParquetField date_field() {
    ParquetField field = leaf(tparquet::Type::INT32);
    tparquet::LogicalType logical_type;
    logical_type.__set_DATE(tparquet::DateType());
    field.schema_element.__set_logicalType(logical_type);
    return field;
}

ParquetField timestamp_micros_field(bool adjusted_to_utc) {
    ParquetField field = leaf(tparquet::Type::INT64);
    tparquet::TimeUnit unit;
    unit.__set_MICROS(tparquet::MicroSeconds());
    tparquet::TimestampType timestamp;
    timestamp.__set_isAdjustedToUTC(adjusted_to_utc);
    timestamp.__set_unit(unit);
    tparquet::LogicalType logical_type;
    logical_type.__set_TIMESTAMP(timestamp);
    field.schema_element.__set_logicalType(logical_type);
    return field;
}

template <typename T>
std::vector<std::string> convert(const ParquetField& field, LogicalType target, const std::vector<T>& raw,
                                 const LegacyCalendar& calendar) {
    std::unique_ptr<ColumnConverter> converter;
    auto type = TypeDescriptor::from_logical_type(target);
    EXPECT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter, calendar));
    auto src = converter->create_src_column();
    src->append_numbers(raw.data(), raw.size() * sizeof(T));
    auto dst = ColumnHelper::create_column(type, true);
    if (converter->need_convert) {
        EXPECT_OK(converter->convert(src.get(), dst.get()));
    } else {
        dst = src->clone();
    }
    std::vector<std::string> result;
    for (size_t i = 0; i < dst->size(); i++) {
        result.push_back(dst->debug_item(i));
    }
    return result;
}

LegacyCalendar legacy_dates() {
    LegacyCalendar calendar;
    calendar.dates = true;
    return calendar;
}
} // namespace

// Expected values: hybrid days from the Julian Day Number of the Julian date, proleptic days from
// Python's datetime.date ordinals.
TEST(CalendarRebaseTest, JulianDaysRebaseToTheSameProlepticDate) {
    EXPECT_EQ(-719162, rebase_julian_to_gregorian_days(-719164)); // 0001-01-01
    EXPECT_EQ(-171499, rebase_julian_to_gregorian_days(-171489)); // 1500-06-15
    EXPECT_EQ(-141438, rebase_julian_to_gregorian_days(-141428)); // 1582-10-04, last Julian day
    EXPECT_EQ(-354226, rebase_julian_to_gregorian_days(-354220)); // 1000-03-01
    EXPECT_EQ(-646115, rebase_julian_to_gregorian_days(-646115)); // 0200-12-31, calendars agree
    // Julian-only leap day 1500-02-29 rolls over to 1500-03-01, as in Spark.
    EXPECT_EQ(-171605, rebase_julian_to_gregorian_days(-171596));
}

TEST(CalendarRebaseTest, GregorianDaysAreUnchanged) {
    EXPECT_EQ(-141427, rebase_julian_to_gregorian_days(-141427)); // 1582-10-15
    EXPECT_EQ(0, rebase_julian_to_gregorian_days(0));
    EXPECT_EQ(19723, rebase_julian_to_gregorian_days(19723));
    // Before Julian Day 0 (4713 BC) values are left as they are.
    EXPECT_EQ(-2440589, rebase_julian_to_gregorian_days(-2440589));
    EXPECT_EQ(std::numeric_limits<int32_t>::min(),
              rebase_julian_to_gregorian_days(std::numeric_limits<int32_t>::min()));
}

TEST(CalendarRebaseTest, FooterRulesFollowSpark) {
    // No writer version: proleptic (StarRocks, Kernel, Trino files).
    EXPECT_FALSE(LegacyCalendar::from_key_value_metadata({}).dates);
    EXPECT_FALSE(LegacyCalendar::from_key_value_metadata({kv("org.apache.spark.legacyDateTime", "")}).dates);
    EXPECT_TRUE(LegacyCalendar::from_key_value_metadata({kv("org.apache.spark.version", "2.4.8")}).dates);
    EXPECT_FALSE(LegacyCalendar::from_key_value_metadata({kv("org.apache.spark.version", "3.0.3")}).dates);
    EXPECT_FALSE(LegacyCalendar::from_key_value_metadata({kv("org.apache.spark.version", "4.2.0")}).dates);
    EXPECT_TRUE(LegacyCalendar::from_key_value_metadata(
                        {kv("org.apache.spark.legacyDateTime", ""), kv("org.apache.spark.version", "3.5.0")})
                        .dates);
    // The INT96 marker alone says nothing about dates.
    EXPECT_FALSE(LegacyCalendar::from_key_value_metadata(
                         {kv("org.apache.spark.version", "3.5.0"), kv("org.apache.spark.legacyINT96", "")})
                         .dates);
}

TEST(CalendarRebaseTest, FileMetaDataReadsTheCalendarFromTheFooter) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement d;
    d.__set_name("d");
    d.__set_type(tparquet::Type::INT32);
    d.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
    tparquet::FileMetaData t_metadata;
    t_metadata.__set_version(1);
    t_metadata.__set_num_rows(0);
    t_metadata.__set_schema({root, d});

    {
        auto copy = t_metadata;
        FileMetaData metadata;
        ASSERT_OK(metadata.init(copy, false));
        EXPECT_FALSE(metadata.legacy_calendar().dates);
    }
    {
        // The footer of a Databricks SQL warehouse (Photon) file holding dates before 1582.
        auto copy = t_metadata;
        copy.__set_key_value_metadata({kv("org.apache.spark.version", "4.2.0"),
                                       kv("org.apache.spark.timeZone", "Etc/UTC"),
                                       kv("org.apache.spark.legacyDateTime", "")});
        FileMetaData metadata;
        ASSERT_OK(metadata.init(copy, false));
        EXPECT_TRUE(metadata.legacy_calendar().dates);
    }
}

TEST(CalendarRebaseTest, LegacyDatesAreRebasedOnRead) {
    // Raw days a Databricks SQL warehouse stored for 0001-01-01, 1500-06-15, 1582-10-04, 1582-10-15 and 2024-01-01.
    const std::vector<int32_t> raw{-719164, -171489, -141428, -141427, 19723};
    EXPECT_EQ((std::vector<std::string>{"0001-01-01", "1500-06-15", "1582-10-04", "1582-10-15", "2024-01-01"}),
              convert(date_field(), TYPE_DATE, raw, legacy_dates()));
    // Without the footer marker the raw days are proleptic and read as they are.
    EXPECT_EQ((std::vector<std::string>{"0000-12-30", "1500-06-25", "1582-10-14", "1582-10-15", "2024-01-01"}),
              convert(date_field(), TYPE_DATE, raw, LegacyCalendar{}));
    // DATE read as DATETIME is rebased too.
    EXPECT_EQ("0001-01-01 00:00:00", convert(date_field(), TYPE_DATETIME, raw, legacy_dates())[0]);
}

TEST(CalendarRebaseTest, LegacyDateReadAsIntegerKeepsItsRawDays) {
    EXPECT_EQ("-719164", convert(date_field(), TYPE_INT, std::vector<int32_t>{-719164}, legacy_dates())[0]);
    // A plain INT32 column is never rebased.
    EXPECT_EQ("-719164",
              convert(leaf(tparquet::Type::INT32), TYPE_BIGINT, std::vector<int32_t>{-719164}, legacy_dates())[0]);
}

TEST(CalendarRebaseTest, GregorianOnlyBatchesPassThrough) {
    const std::vector<int32_t> raw{-141427, 0, 19723};
    EXPECT_EQ((std::vector<std::string>{"1582-10-15", "1970-01-01", "2024-01-01"}),
              convert(date_field(), TYPE_DATE, raw, legacy_dates()));
}

// Databricks stores TIMESTAMP as INT96 and TIMESTAMP_NTZ as INT64 without rebasing, so timestamps are read as before.
TEST(CalendarRebaseTest, TimestampsAreNotRebased) {
    constexpr int64_t MICROS_PER_DAY = 86400LL * 1000000;
    const std::vector<int64_t> micros{-171499 * MICROS_PER_DAY};
    EXPECT_EQ("1500-06-15 00:00:00", convert(timestamp_micros_field(true), TYPE_DATETIME, micros, legacy_dates())[0]);
    EXPECT_EQ("1500-06-15 00:00:00", convert(timestamp_micros_field(false), TYPE_DATETIME, micros, legacy_dates())[0]);
    const std::vector<int96_t> int96{{0, static_cast<uint32_t>(-171499 + 2440588)}};
    EXPECT_EQ("1500-06-15 00:00:00", convert(leaf(tparquet::Type::INT96), TYPE_DATETIME, int96, legacy_dates())[0]);
}

// Row-group and page-index min/max go through the same converter.
TEST(CalendarRebaseTest, DateStatisticsAreRebased) {
    auto encode = [](int32_t days) {
        std::string value(sizeof(days), '\0');
        memcpy(value.data(), &days, sizeof(days));
        return value;
    };
    const auto field = date_field();
    const auto type = TypeDescriptor::from_logical_type(TYPE_DATE);
    auto min_column = ColumnHelper::create_column(type, true);
    auto max_column = ColumnHelper::create_column(type, true);
    ASSERT_OK(StatisticsHelper::decode_min_max_into_columns(min_column, max_column, {encode(-719164)}, {encode(19723)},
                                                            {false}, type, &field, "UTC", legacy_dates()));
    EXPECT_EQ("0001-01-01", min_column->debug_item(0));
    EXPECT_EQ("2024-01-01", max_column->debug_item(0));
}

} // namespace starrocks::parquet
