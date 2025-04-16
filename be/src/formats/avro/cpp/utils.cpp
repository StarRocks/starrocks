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

#include "formats/avro/cpp/utils.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace starrocks {

static Status datum_to_rapidjson(const avro::GenericDatum& datum, rapidjson::Document::AllocatorType& allocator,
                                 rapidjson::Value& out) {
    switch (datum.type()) {
    case avro::AVRO_STRING: {
        out.SetString(datum.value<std::string>().c_str(), allocator);
        return Status::OK();
    }

    case avro::AVRO_BYTES: {
        const auto& from = datum.value<std::vector<uint8_t>>();
        out.SetString(reinterpret_cast<const char*>(from.data()), from.size(), allocator);
        return Status::OK();
    }

    case avro::AVRO_INT: {
        out.SetInt(datum.value<int32_t>());
        return Status::OK();
    }

    case avro::AVRO_LONG: {
        out.SetInt64(datum.value<int64_t>());
        return Status::OK();
    }

    case avro::AVRO_FLOAT: {
        out.SetFloat(datum.value<float>());
        return Status::OK();
    }

    case avro::AVRO_DOUBLE: {
        out.SetDouble(datum.value<double>());
        return Status::OK();
    }

    case avro::AVRO_BOOL: {
        out.SetBool(datum.value<bool>());
        return Status::OK();
    }

    case avro::AVRO_NULL: {
        out.SetNull();
        return Status::OK();
    }

    case avro::AVRO_RECORD: {
        out.SetObject();

        const auto& record = datum.value<avro::GenericRecord>();
        const auto& schema = record.schema();
        for (size_t i = 0; i < record.fieldCount(); ++i) {
            const auto& field = record.fieldAt(i);
            const auto& field_name = schema->nameAt(i);

            rapidjson::Value field_name_val;
            field_name_val.SetString(field_name.c_str(), allocator);
            rapidjson::Value field_value_val;
            RETURN_IF_ERROR(datum_to_rapidjson(field, allocator, field_value_val));
            out.AddMember(field_name_val, field_value_val, allocator);
        }
        return Status::OK();
    }

    case avro::AVRO_ENUM: {
        const auto& from = datum.value<avro::GenericEnum>().symbol();
        out.SetString(from.c_str(), allocator);
        return Status::OK();
    }

    case avro::AVRO_ARRAY: {
        out.SetArray();

        const auto& array = datum.value<avro::GenericArray>();
        const auto& array_values = array.value();
        for (auto& value : array_values) {
            rapidjson::Value element_value;
            RETURN_IF_ERROR(datum_to_rapidjson(value, allocator, element_value));
            out.PushBack(element_value, allocator);
        }
        return Status::OK();
    }

    case avro::AVRO_MAP: {
        out.SetObject();

        const auto& map = datum.value<avro::GenericMap>();
        const auto& map_values = map.value();
        for (auto& p : map_values) {
            const auto& key = p.first;
            rapidjson::Value key_val;
            key_val.SetString(key.c_str(), allocator);

            const auto& value = p.second;
            rapidjson::Value value_val;
            RETURN_IF_ERROR(datum_to_rapidjson(value, allocator, value_val));

            out.AddMember(key_val, value_val, allocator);
        }
        return Status::OK();
    }

    case avro::AVRO_UNION: {
        RETURN_IF_ERROR(datum_to_rapidjson(datum.value<avro::GenericUnion>().datum(), allocator, out));
        return Status::OK();
    }

    case avro::AVRO_FIXED: {
        const auto& fixed = datum.value<avro::GenericFixed>();
        const auto& from = fixed.value();
        out.SetString(reinterpret_cast<const char*>(from.data()), from.size(), allocator);
        return Status::OK();
    }

    default:
        return Status::NotSupported(
                fmt::format("Unsupported avro type {} to json", avro::toString(datum.type())));
    }
}

Status AvroUtils::datum_to_json(const avro::GenericDatum& datum, std::string* json_str) {
    rapidjson::Document doc;
    auto& allocator = doc.GetAllocator();
    rapidjson::Value root;
    RETURN_IF_ERROR(datum_to_rapidjson(datum, allocator, root));

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    root.Accept(writer);
    json_str->append(buffer.GetString(), buffer.GetSize());
    return Status::OK();
}

std::string AvroUtils::logical_type_to_string(const avro::LogicalType& logical_type) {
    switch (logical_type.type()) {
    case avro::LogicalType::NONE:
        return "none";
    case avro::LogicalType::DECIMAL:
        return fmt::format("decimal({},{})", logical_type.precision(), logical_type.scale());
    case avro::LogicalType::DATE:
        return "date";
    case avro::LogicalType::TIME_MILLIS:
        return "time-millis";
    case avro::LogicalType::TIME_MICROS:
        return "time-micros";
    case avro::LogicalType::TIMESTAMP_MILLIS:
        return "timestamp-millis";
    case avro::LogicalType::TIMESTAMP_MICROS:
        return "timestamp-micros";
    case avro::LogicalType::DURATION:
        return "duration";
    case avro::LogicalType::UUID:
        return "uuid";
    default:
        return "unknown";
    }

}

} // namespace starrocks
