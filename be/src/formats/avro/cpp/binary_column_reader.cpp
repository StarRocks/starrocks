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

#include "column/binary_column.h"
#include "formats/avro/cpp/utils.h"

namespace starrocks::avrocpp {

Status BinaryColumnReader::read_datum(const avro::GenericDatum& datum, Column* column) {
    auto binary_column = down_cast<BinaryColumn*>(column);

    switch (datum.type()) {
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return read_numeric_value(datum, binary_column);

    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_FIXED:
        return read_string_value(datum, binary_column);

    case avro::AVRO_ENUM:
        return read_enum_value(datum, binary_column);

    case avro::AVRO_RECORD:
    case avro::AVRO_ARRAY:
    case avro::AVRO_MAP:
        return read_complex_value(datum, binary_column);

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to string. column: {}", avro::toString(datum.type()), _col_name));
    }
}

static inline Status check_append_binary_column(const std::string& from, const std::string& col_name,
                                                const TypeDescriptor& type_desc, BinaryColumn* column) {
    if (UNLIKELY(from.size() > type_desc.len)) {
        return Status::DataQualityError(
                fmt::format("Value length is beyond the capacity. column: {}, capacity: {}", col_name, type_desc.len));
    }

    column->append(Slice(from));
    return Status::OK();
}

Status BinaryColumnReader::read_numeric_value(const avro::GenericDatum& datum, BinaryColumn* column) {
    switch (datum.type()) {
    case avro::AVRO_INT: {
        const auto& from = datum.value<int32_t>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, _col_name, _type_desc, column);
    }

    case avro::AVRO_LONG: {
        const auto& from = datum.value<int64_t>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, _col_name, _type_desc, column);
    }

    case avro::AVRO_FLOAT: {
        const auto& from = datum.value<float>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, _col_name, _type_desc, column);
    }

    case avro::AVRO_DOUBLE: {
        const auto& from = datum.value<double>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, _col_name, _type_desc, column);
    }

    case avro::AVRO_BOOL: {
        const auto& from = datum.value<bool>();
        column->append(from ? Slice("1") : Slice("0"));
        return Status::OK();
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to string. column: {}", avro::toString(datum.type()), _col_name));
    }
}

Status BinaryColumnReader::read_string_value(const avro::GenericDatum& datum, BinaryColumn* column) {
    switch (datum.type()) {
    case avro::AVRO_STRING: {
        const auto& from = datum.value<std::string>();
        return check_append_binary_column(from, _col_name, _type_desc, column);
    }

    case avro::AVRO_BYTES: {
        const auto& from = datum.value<std::vector<uint8_t>>();
        return check_append_binary_column(std::string(from.begin(), from.end()), _col_name, _type_desc, column);
    }

    case avro::AVRO_FIXED: {
        const auto& from = datum.value<avro::GenericFixed>();
        const auto& v = from.value();
        return check_append_binary_column(std::string(v.begin(), v.end()), _col_name, _type_desc, column);
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to string. column: {}", avro::toString(datum.type()), _col_name));
    }
}

Status BinaryColumnReader::read_enum_value(const avro::GenericDatum& datum, BinaryColumn* column) {
    const auto& from = datum.value<avro::GenericEnum>();
    const auto& v = from.symbol();
    return check_append_binary_column(v, _col_name, _type_desc, column);
}

Status BinaryColumnReader::read_complex_value(const avro::GenericDatum& datum, BinaryColumn* column) {
    auto type = datum.type();
    DCHECK(type == avro::AVRO_RECORD || type == avro::AVRO_ARRAY || type == avro::AVRO_MAP);

    std::string json_str;
    RETURN_IF_ERROR(AvroUtils::datum_to_json(datum, &json_str, true, _timezone));
    return check_append_binary_column(json_str, _col_name, _type_desc, column);
}

} // namespace starrocks::avrocpp
