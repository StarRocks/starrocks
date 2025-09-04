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

#include "variant_util.h"

#include <arrow/util/endian.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "exprs/cast_expr.h"
#include "formats/parquet/variant.h"
#include "runtime/decimalv3.h"
#include "types/timestamp_value.h"
#include "url_coding.h"

namespace starrocks {

uint32_t VariantUtil::readLittleEndianUnsigned(const void* from, uint8_t size) {
    uint32_t result = 0;
    memcpy(&result, from, size);
    return arrow::bit_util::FromLittleEndian(result);
}

std::string VariantUtil::type_to_string(VariantType type) {
    switch (type) {
    case VariantType::OBJECT:
        return "Object";
    case VariantType::ARRAY:
        return "Array";
    case VariantType::NULL_TYPE:
        return "Null";
    case VariantType::BOOLEAN:
        return "Boolean";
    case VariantType::INT8:
        return "Int8";
    case VariantType::INT16:
        return "Int16";
    case VariantType::INT32:
        return "Int32";
    case VariantType::INT64:
        return "Int64";
    case VariantType::DOUBLE:
        return "Double";
    case VariantType::DECIMAL4:
        return "Decimal4";
    case VariantType::DECIMAL8:
        return "Decimal8";
    case VariantType::DECIMAL16:
        return "Decimal16";
    case VariantType::DATE:
        return "Date";
    case VariantType::TIMESTAMP_TZ:
        return "TimestampTZ";
    case VariantType::TIMESTAMP_NTZ:
        return "TimestampNTZ";
    case VariantType::FLOAT:
        return "Float";
    case VariantType::BINARY:
        return "Binary";
    case VariantType::STRING:
        return "String";
    case VariantType::TIME_NTZ:
        return "TimeNTZ";
    case VariantType::TIMESTAMP_TZ_NANOS:
        return "TimestampTZNanos";
    case VariantType::TIMESTAMP_NTZ_NANOS:
        return "TimestampNTZNanos";
    case VariantType::UUID:
        return "UUID";
    default:
        return "Unknown";
    }
}

std::string epoch_day_to_date(int32_t epoch_days) {
    std::time_t raw_time = epoch_days * 86400; // to seconds
    std::tm* ptm = std::gmtime(&raw_time);     // to UTC
    char buffer[11];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", ptm);
    return buffer;
}

std::string VariantUtil::decimal4_to_string(DecimalValue<int32_t> decimal) {
    return DecimalV3Cast::to_string<int32_t>(decimal.value, decimal_precision_limit<int32_t>, decimal.scale);
}

std::string VariantUtil::decimal8_to_string(DecimalValue<int64_t> decimal) {
    return DecimalV3Cast::to_string<int64_t>(decimal.value, decimal_precision_limit<int64_t>, decimal.scale);
}

std::string VariantUtil::decimal16_to_string(DecimalValue<int128_t> decimal) {
    return DecimalV3Cast::to_string<int128_t>(decimal.value, decimal_precision_limit<int128_t>, decimal.scale);
}

void append_quoted_string(std::stringstream& ss, const std::string& str) {
    ss << '"' << str << '"';
}

Status VariantUtil::variant_to_json(std::string_view metadata, std::string_view value, std::stringstream& json_str,
                                    cctz::time_zone timezone) {
    Variant variant{metadata, value};
    switch (variant.type()) {
    case VariantType::NULL_TYPE:
        json_str << "null";
        break;
    case VariantType::BOOLEAN: {
        bool res = *variant.get_bool();
        json_str << (res ? "true" : "false");
        break;
    }
    case VariantType::INT8:
        json_str << std::to_string(*variant.get_int8());
        break;
    case VariantType::INT16:
        json_str << std::to_string(*variant.get_int16());
        break;
    case VariantType::INT32:
        json_str << std::to_string(*variant.get_int32());
        break;
    case VariantType::INT64:
        json_str << std::to_string(*variant.get_int64());
        break;
    case VariantType::FLOAT: {
        const float f = *variant.get_float();
        if (std::isfinite(f)) {
            json_str << std::to_string(f);
        } else {
            append_quoted_string(json_str, std::to_string(f));
        }
        break;
    }
    case VariantType::DOUBLE: {
        const double d = *variant.get_double();
        if (std::isfinite(d)) {
            json_str << std::to_string(d);
        } else {
            append_quoted_string(json_str, std::to_string(d));
        }
        break;
    }
    case VariantType::DECIMAL4: {
        DecimalValue<int32_t> decimal = *variant.get_decimal4();
        json_str << decimal4_to_string(decimal);
        break;
    }
    case VariantType::DECIMAL8: {
        DecimalValue<int64_t> decimal = *variant.get_decimal8();
        json_str << decimal8_to_string(decimal);
        break;
    }
    case VariantType::DECIMAL16: {
        DecimalValue<int128_t> decimal = *variant.get_decimal16();
        json_str << decimal16_to_string(decimal);
        break;
    }
    case VariantType::STRING: {
        json_str << *variant.get_string();
        break;
    }
    case VariantType::BINARY: {
        const std::string_view binary = *variant.get_binary();
        const std::string binary_str(binary.data(), binary.size());
        std::string encoded;
        base64_encode(binary_str, &encoded);
        append_quoted_string(json_str, encoded);
        break;
    }
    case VariantType::UUID: {
        const auto uuid_arr = *variant.get_uuid();
        boost::uuids::uuid uuid{};
        for (size_t i = 0; i < uuid.size(); ++i) {
            uuid.data[i] = uuid_arr[i];
        }
        append_quoted_string(json_str, boost::uuids::to_string(uuid));
        break;
    }
    case VariantType::DATE: {
        int32_t date = *variant.get_date();
        std::string date_str = epoch_day_to_date(date);
        append_quoted_string(json_str, date_str);
        break;
    }
    case VariantType::TIMESTAMP_TZ: {
        const int64_t timestamp_micros = *variant.get_timestamp_micros();
        TimestampValue tsv{};
        tsv.from_unix_second(timestamp_micros / 1000000, timestamp_micros % 1000000);
        std::string timestamp_str = timestamp::to_string_with_timezone<false, false>(tsv.timestamp(), timezone);
        append_quoted_string(json_str, timestamp_str);
        break;
    }
    case VariantType::TIMESTAMP_NTZ: {
        const int64_t timestamp_micros = *variant.get_timestamp_micros_ntz();
        TimestampValue tsv{};
        tsv.from_unix_second(timestamp_micros / 1000000, timestamp_micros % 1000000);
        std::string timestamp_str = tsv.to_string(false);
        append_quoted_string(json_str, timestamp_str);
        break;
    }
    case VariantType::OBJECT: {
        auto info = get_object_info(value);
        if (!info.ok()) {
            return info.status();
        }
        const auto& [num_elements, id_start_offset, id_size, offset_start_offset, offset_size, data_start_offset] =
                info.value();
        json_str << "{";
        for (size_t i = 0; i < num_elements; ++i) {
            if (i > 0) {
                json_str << ",";
            }

            uint32_t id = readLittleEndianUnsigned(value.data() + id_start_offset + i * id_size, id_size);
            uint32_t offset =
                    readLittleEndianUnsigned(value.data() + offset_start_offset + i * offset_size, offset_size);
            auto key = variant.metadata().get_key(id);
            if (!key.ok()) {
                return key.status();
            }

            json_str << *key << ":";

            if (uint32_t next_pos = data_start_offset + offset; next_pos < value.size()) {
                std::string_view next_value = value.substr(next_pos, value.size() - next_pos);
                // Recursively convert the next value to JSON
                auto status = variant_to_json(metadata, next_value, json_str, timezone);
                if (!status.ok()) {
                    return status;
                }
            } else {
                return Status::InternalError("Invalid offset in object: " + std::to_string(offset));
            }
        }
        json_str << "}";
        break;
    }
    case VariantType::ARRAY: {
        auto info = get_array_info(value);
        if (!info.ok()) {
            return info.status();
        }

        const auto& [num_elements, offset_size, offset_start_offset, data_start_offset] = info.value();
        json_str << "[";
        for (size_t i = 0; i < num_elements; ++i) {
            if (i > 0) {
                json_str << ",";
            }

            uint32_t offset =
                    readLittleEndianUnsigned(value.data() + offset_start_offset + i * offset_size, offset_size);
            if (uint32_t next_pos = data_start_offset + offset; next_pos < value.size()) {
                std::string_view next_value = value.substr(next_pos, value.size() - next_pos);
                // Recursively convert the next value to JSON
                auto status = variant_to_json(metadata, next_value, json_str, timezone);
                if (!status.ok()) {
                    return status;
                }
            } else {
                return Status::InternalError("Invalid offset in array: " + std::to_string(offset));
            }
        }
        json_str << "]";
        break;
    }
    default:
        return Status::NotSupported("Unsupported variant type: " + type_to_string(variant.type()));
    }

    return Status::OK();
}

uint8_t VariantUtil::primitiveHeader(VariantPrimitiveType primitive) {
    return static_cast<uint8_t>(primitive) << 2;
}

} // namespace starrocks
