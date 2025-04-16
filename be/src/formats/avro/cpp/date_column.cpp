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

#include "formats/avro/cpp/binary_column.h"

#include <chrono>
#include <ctime>

#include "column/fixed_length_column.h"
#include "formats/avro/cpp/utils.h"

namespace starrocks {

// ------ date column ------

static Status add_date_column_with_int_value(const avro::GenericDatum& datum, const std::string& col_name,
                                             FixedLengthColumn<DateValue>* column) {
    if (datum.logicalType().type() != avro::LogicalType::DATE) {
        return Status::NotSupported(fmt::format("Logical type of {} is {}. column: {}", avro::toString(datum.type()),
                                                AvroUtils::logical_type_to_string(datum.logicalType()), col_name));
    }

    const auto& from = datum.value<int32_t>();

    auto time = static_cast<std::time_t>(from) * 86400;
    std::tm* tm = std::gmtime(&time);

    column->append(DateValue::create(tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday));
    return Status::OK();
}

static Status add_date_column_with_string_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                FixedLengthColumn<DateValue>* column) {
    const auto& from = datum.value<std::string>();

    DateValue to{};
    bool success = to.from_string(from.data(), from.size());
    if (!success) {
        return Status::DataQualityError(fmt::format("Avro string to date failed. column: {}", col_name));
    }

    column->append(to);
    return Status::OK();
}

Status add_date_column(const avro::GenericDatum& datum, const std::string& col_name, Column* column) {
    auto date_column = down_cast<FixedLengthColumn<DateValue>*>(column);

    switch (datum.type()) {
    case avro::AVRO_INT:
        return add_date_column_with_int_value(datum, col_name, date_column);

    case avro::AVRO_STRING:
        return add_date_column_with_string_value(datum, col_name, date_column);

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to date. column: {}", avro::toString(datum.type()), col_name));
    }
}

// ------ datetime column ------

static Status add_datetime_column_with_long_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                  const cctz::time_zone& timezone,
                                                  FixedLengthColumn<TimestampValue>* column) {
    const auto& from = datum.value<int64_t>();

    TimestampValue to{};
    switch (datum.logicalType().type()) {
    case avro::LogicalType::TIMESTAMP_MILLIS: {
        to.from_unixtime(from / 1000, (from % 1000) * 1000, timezone);
        column->append(to);
        return Status::OK();
    }

    case avro::LogicalType::TIMESTAMP_MICROS: {
        to.from_unixtime(from / 1000000, (from % 1000000), timezone);
        column->append(to);
        return Status::OK();
    }

    default:
        return Status::NotSupported(fmt::format("Logical type of {} is {}, column: {}", avro::toString(datum.type()),
                                                AvroUtils::logical_type_to_string(datum.logicalType()), col_name));
    }
}

static Status add_datetime_column_with_string_value(const avro::GenericDatum& datum, const std::string& col_name,
                                                    FixedLengthColumn<TimestampValue>* column) {
    const auto& from = datum.value<std::string>();

    TimestampValue to{};
    bool success = to.from_string(from.data(), from.size());
    if (!success) {
        return Status::DataQualityError(fmt::format("Avro string to datetime failed. column: {}", col_name));
    }

    column->append(to);
    return Status::OK();
}

Status add_datetime_column(const avro::GenericDatum& datum, const std::string& col_name,
                           const cctz::time_zone& timezone, Column* column) {
    auto datetime_column = down_cast<FixedLengthColumn<TimestampValue>*>(column);

    switch (datum.type()) {
    case avro::AVRO_LONG:
        return add_datetime_column_with_long_value(datum, col_name, timezone, datetime_column);

    case avro::AVRO_STRING:
        return add_datetime_column_with_string_value(datum, col_name, datetime_column);

    default:
        return Status::NotSupported(fmt::format("Unsupported avro type {} to datetime. column: {}",
                                                avro::toString(datum.type()), col_name));
    }
}

} // namespace starrocks
