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

#include "formats/avro/cpp/utils.h"

namespace starrocks::avrocpp {

Status DateColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    auto date_column = down_cast<FixedLengthColumn<DateValue>*>(column);

    switch (datum.type()) {
    case avro::AVRO_INT:
        return read_int_value(datum, date_column);

    case avro::AVRO_STRING:
        return read_string_value(datum, date_column);

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to date. column: {}", avro::toString(datum.type()), _col_name));
    }
}

Status DateColumnReader::read_int_value(const avro::GenericDatum& datum, FixedLengthColumn<DateValue>* column) {
    if (datum.logicalType().type() != avro::LogicalType::DATE) {
        return Status::NotSupported(fmt::format("Logical type of {} is {}. column: {}", avro::toString(datum.type()),
                                                AvroUtils::logical_type_to_string(datum.logicalType()), _col_name));
    }

    const auto& from = datum.value<int32_t>();
    column->append(AvroUtils::int_to_date_value(from));
    return Status::OK();
}

Status DateColumnReader::read_string_value(const avro::GenericDatum& datum, FixedLengthColumn<DateValue>* column) {
    const auto& from = datum.value<std::string>();

    DateValue to{};
    bool success = to.from_string(from.data(), from.size());
    if (!success) {
        return Status::DataQualityError(fmt::format("Avro string to date failed. column: {}", _col_name));
    }

    column->append(to);
    return Status::OK();
}

Status DatetimeColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    auto datetime_column = down_cast<FixedLengthColumn<TimestampValue>*>(column);

    switch (datum.type()) {
    case avro::AVRO_LONG:
        return read_long_value(datum, datetime_column);

    case avro::AVRO_STRING:
        return read_string_value(datum, datetime_column);

    default:
        return Status::NotSupported(fmt::format("Unsupported avro type {} to datetime. column: {}",
                                                avro::toString(datum.type()), _col_name));
    }
}

Status DatetimeColumnReader::read_long_value(const avro::GenericDatum& datum,
                                             FixedLengthColumn<TimestampValue>* column) {
    auto timestamp_value_or = AvroUtils::long_to_timestamp_value(datum, _timezone);
    if (!timestamp_value_or.ok()) {
        return timestamp_value_or.status().clone_and_append(fmt::format("column: {}", _col_name));
    }

    column->append(timestamp_value_or.value());
    return Status::OK();
}

Status DatetimeColumnReader::read_string_value(const avro::GenericDatum& datum,
                                               FixedLengthColumn<TimestampValue>* column) {
    const auto& from = datum.value<std::string>();

    TimestampValue to{};
    bool success = to.from_string(from.data(), from.size());
    if (!success) {
        return Status::DataQualityError(fmt::format("Avro string to datetime failed. column: {}", _col_name));
    }

    column->append(to);
    return Status::OK();
}

} // namespace starrocks::avrocpp
