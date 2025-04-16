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

#include "column/binary_column.h"
#include "formats/avro/cpp/utils.h"

namespace starrocks {

static inline Status check_append_binary_column(const std::string& from, const std::string& col_name,
                                                const TypeDescriptor& type_desc, BinaryColumn* column) {
    if (UNLIKELY(type_desc.len < from.size())) {
        return Status::DataQualityError(fmt::format(
                "Value length is beyond the capacity. column: {}, capacity: {}", col_name, type_desc.len));
    }

    column->append(Slice(from));
    return Status::OK();
}

static Status add_column_with_numeric_value(const avro::GenericDatum& datum, const std::string& col_name,
                                            const TypeDescriptor& type_desc, BinaryColumn* column) {
    switch (datum.type()) {
    case avro::AVRO_INT: {
        const auto& from = datum.value<int32_t>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, col_name, type_desc, column);
    }

    case avro::AVRO_LONG: {
        const auto& from = datum.value<int64_t>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, col_name, type_desc, column);
    }

    case avro::AVRO_FLOAT: {
        const auto& from = datum.value<float>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, col_name, type_desc, column);
    }

    case avro::AVRO_DOUBLE: {
        const auto& from = datum.value<double>();
        auto sv = std::to_string(from);
        return check_append_binary_column(sv, col_name, type_desc, column);
    }

    case avro::AVRO_BOOL: {
        const auto& from = datum.value<bool>();
        column->append(from ? Slice("1") : Slice("0"));
        return Status::OK();
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to string. column: {}", avro::toString(datum.type()), col_name));
    }
}

static Status add_column_with_string_value(const avro::GenericDatum& datum, const std::string& col_name,
                                           const TypeDescriptor& type_desc, BinaryColumn* column) {
    switch (datum.type()) {
    case avro::AVRO_STRING: {
        const auto& from = datum.value<std::string>();
        return check_append_binary_column(from, col_name, type_desc, column);
    }

    case avro::AVRO_BYTES: {
        const auto& from = datum.value<std::vector<uint8_t>>();
        return check_append_binary_column(std::string(from.begin(), from.end()), col_name, type_desc, column);
    }

    case avro::AVRO_FIXED: {
        const auto& from = datum.value<avro::GenericFixed>();
        const auto& v = from.value();
        return check_append_binary_column(std::string(v.begin(), v.end()), col_name, type_desc, column);
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to string. column: {}", avro::toString(datum.type()), col_name));
    }
}

static Status add_column_with_enum_value(const avro::GenericDatum& datum, const std::string& col_name,
                                           const TypeDescriptor& type_desc, BinaryColumn* column) {
    const auto& from = datum.value<avro::GenericEnum>();
    const auto& v = from.symbol();
    return check_append_binary_column(v, col_name, type_desc, column);
}

static Status add_column_with_complex_value(const avro::GenericDatum& datum, const std::string& col_name,
                                            const TypeDescriptor& type_desc, BinaryColumn* column) {
    auto type = datum.type();
    DCHECK(type == avro::AVRO_RECORD || type == avro::AVRO_ARRAY || type == avro::AVRO_MAP);

    std::string json_str;
    RETURN_IF_ERROR(AvroUtils::datum_to_json(datum, &json_str));
    return check_append_binary_column(json_str, col_name, type_desc, column);
}

Status add_binary_column(const avro::GenericDatum& datum, const std::string& col_name, const TypeDescriptor& type_desc,
                         Column* column) {
    auto binary_column = down_cast<BinaryColumn*>(column);

    switch (datum.type()) {
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return add_column_with_numeric_value(datum, col_name, type_desc, binary_column);

    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_FIXED:
        return add_column_with_string_value(datum, col_name, type_desc, binary_column);

    case avro::AVRO_ENUM:
        return add_column_with_enum_value(datum, col_name, type_desc, binary_column);

    case avro::AVRO_RECORD:
    case avro::AVRO_ARRAY:
    case avro::AVRO_MAP:
        return add_column_with_complex_value(datum, col_name, type_desc, binary_column);

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to string. column: {}", avro::toString(datum.type()), col_name));
    }
}

} // namespace starrocks
